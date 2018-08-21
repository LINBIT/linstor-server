package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.makeRscContext;

@Singleton
public class CtrlRscToggleDiskApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlVlmCrtApiHelper ctrlVlmCrtApiHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlRscToggleDiskApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlVlmCrtApiHelper ctrlVlmCrtApiHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlVlmCrtApiHelper = ctrlVlmCrtApiHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        peerAccCtx = peerAccCtxRef;
    }

    @Override
    public Flux<ApiCallRc> satelliteConnected(Node node)
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        try
        {
            Iterator<Resource> localRscIter = node.iterateResources(apiCtx);
            while (localRscIter.hasNext())
            {
                Resource localRsc = localRscIter.next();
                ResourceDefinition rscDfn = localRsc.getDefinition();

                Iterator<Resource> rscIter = rscDfn.iterateResource(apiCtx);
                while (rscIter.hasNext())
                {
                    Resource rsc = rscIter.next();
                    if (rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISK_ADD_REQUESTED))
                    {
                        fluxes.add(Flux
                            .defer(() -> updateAndAddDisk(node.getName(), rscDfn.getName()))
                            .doOnError(exc ->
                                errorReporter.reportError(
                                    exc,
                                    null,
                                    null,
                                    "Failed to continue adding disk for " + rsc
                                )
                            )
                            .onErrorResume(ignored -> Flux.empty())
                        );
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

        return Flux.merge(fluxes);
    }

    public Flux<ApiCallRc> resourceToggleDisk(
        String nodeNameStr,
        String rscNameStr,
        String storPoolNameStr
    )
    {
        ResponseContext context = makeRscContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            rscNameStr
        );

        return Flux
            .defer(() ->
                scopeRunner
                    .fluxInTransactionalScope(
                        createLockGuard(),
                        () -> toggleDiskInTransaction(nodeNameStr, rscNameStr, storPoolNameStr)
                    )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> toggleDiskInTransaction(
        String nodeNameStr,
        String rscNameStr,
        String storPoolNameStr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);

        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeName.displayValue, rscName.displayValue, true);

        if (!ctrlVlmCrtApiHelper.isDiskless(rsc))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_RSC_ALREADY_HAS_DISK,
                "Resource already has disk"
            ));
        }

        // Save the requested storage pool in the resource properties.
        // This does not cause the storage pool to be used automatically.
        Props rscProps = ctrlPropsHelper.getProps(rsc);
        if (storPoolNameStr == null || storPoolNameStr.isEmpty())
        {
            rscProps.map().remove(ApiConsts.KEY_STOR_POOL_NAME);
        }
        else
        {
            ctrlPropsHelper.fillProperties(
                LinStorObject.RESOURCE,
                Collections.singletonMap(ApiConsts.KEY_STOR_POOL_NAME, storPoolNameStr),
                rscProps,
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }

        // Resolve storage pool now so that nothing is committed if the storage pool configuration is invalid
        Iterator<Volume> vlmIter = rsc.iterateVolumes();
        while (vlmIter.hasNext())
        {
            VolumeDefinition vlmDfn = vlmIter.next().getVolumeDefinition();

            ctrlVlmCrtApiHelper.resolveStorPool(rsc, vlmDfn, false).extractApiCallRc(responses);
        }

        markDiskAddRequested(rsc);

        ctrlTransactionHelper.commit();

        responses.addEntry(ApiCallRcImpl.simpleEntry(
            ApiConsts.MODIFIED,
            "Addition of disk to resource '" + rsc.getDefinition().getName().displayValue + "' " +
                "on node '" + rsc.getAssignedNode().getName().displayValue + "' registered"
        ));

        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(Flux.defer(() -> updateAndAddDisk(nodeName, rscName)));
    }

    // Restart from here when connection established and flag set
    private Flux<ApiCallRc> updateAndAddDisk(NodeName nodeName, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                createLockGuard(),
                () -> updateAndAddDiskInTransaction(nodeName, rscName)
            );
    }

    private Flux<ApiCallRc> updateAndAddDiskInTransaction(NodeName nodeName, ResourceName rscName)
    {
        Flux<ApiCallRc> flux;

        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeName.displayValue, rscName.displayValue, true);

        // Avoid performing the operation multiple times simultaneously
        if (hasDiskAdding(rsc))
        {
            flux = Flux.empty();
        }
        else
        {
            ApiCallRcImpl offlineWarnings = new ApiCallRcImpl();

            try
            {
                Iterator<Resource> rscIterator = rsc.getDefinition().iterateResource(apiCtx);
                while (rscIterator.hasNext())
                {
                    Resource currentRsc = rscIterator.next();
                    Node node = currentRsc.getAssignedNode();
                    if (!node.getPeer(apiCtx).isConnected())
                    {
                        offlineWarnings.addEntry(ResponseUtils.makeNotConnectedWarning(node.getName()));
                    }
                }
            }
            catch (AccessDeniedException implError)
            {
                throw new ImplementationError(implError);
            }

            // Don't start the operation if any of the required nodes are offline
            if (!offlineWarnings.getEntries().isEmpty())
            {
                flux = Flux.just(offlineWarnings);
            }
            else
            {
                markDiskAdding(rsc);
                ctrlTransactionHelper.commit();

                Flux<ApiCallRc> satelliteUpdateResponses;
                if (rsc.getVolumeCount() > 0)
                {
                    satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(rsc)
                        .transform(updateResponses -> translateDeploymentSuccess(updateResponses,
                            "Prepared '%s' to expect disk on '" +
                                rsc.getAssignedNode().getName().displayValue + "'"));
                }
                else
                {
                    satelliteUpdateResponses = Flux.empty();
                }

                flux = satelliteUpdateResponses
                    // If an update fails (e.g. the connection to a node is lost), attempt to reset back to the initial
                    // state. The requested flag is not reset, so the operation will be retried when the nodes are next
                    // all connected.
                    .onErrorResume(error ->
                        reset(nodeName, rscName)
                            .concatWith(Flux.error(error))
                    )
                    .concatWith(Flux.defer(() -> addDisk(nodeName, rscName)))
                    .onErrorResume(CtrlSatelliteUpdateCaller.DelayedApiRcException.class, ignored -> Flux.empty());
            }
        }

        return flux;
    }

    private Flux<ApiCallRc> reset(NodeName nodeName, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                createLockGuard(),
                () -> resetInTransaction(nodeName, rscName)
            );
    }

    private Flux<ApiCallRc> resetInTransaction(
        NodeName nodeName,
        ResourceName rscName
    )
    {
        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeName.displayValue, rscName.displayValue, true);

        unmarkDiskAdding(rsc);

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(rsc)
            .transform(responses -> translateDeploymentSuccess(responses, "Diskless state temporarily reset on '%s'"));

        return satelliteUpdateResponses
            .onErrorResume(CtrlSatelliteUpdateCaller.DelayedApiRcException.class, ignored -> Flux.empty());
    }

    private Flux<ApiCallRc> addDisk(NodeName nodeName, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                createLockGuard(),
                () -> addDiskInTransaction(nodeName, rscName)
            );
    }

    private Flux<ApiCallRc> addDiskInTransaction(
        NodeName nodeName,
        ResourceName rscName
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeName.displayValue, rscName.displayValue, true);

        markDiskAdded(rsc);

        Iterator<Volume> vlmIter = rsc.iterateVolumes();
        while (vlmIter.hasNext())
        {
            Volume vlm = vlmIter.next();

            StorPool storPool =
                ctrlVlmCrtApiHelper.resolveStorPool(rsc, vlm.getVolumeDefinition(), false).extractApiCallRc(responses);

            setStorPool(vlm, storPool);
        }

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(rsc)
            .transform(updateResponses -> translateDeploymentSuccess(updateResponses,
                "Notified '%s' of addition of new disk on '" + nodeName.displayValue + "'"));

        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(satelliteUpdateResponses);
    }

    private void markDiskAddRequested(Resource rsc)
    {
        try
        {
            rsc.getStateFlags().enableFlags(peerAccCtx.get(), Resource.RscFlags.DISK_ADD_REQUESTED);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getRscDescription(rsc) + " adding disk",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private boolean hasDiskAdding(Resource rsc)
    {
        boolean diskAdding;
        try
        {
            diskAdding = rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISK_ADDING);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return diskAdding;
    }

    private void markDiskAdding(Resource rsc)
    {
        try
        {
            rsc.getStateFlags().enableFlags(apiCtx, Resource.RscFlags.DISK_ADDING);
        }
        catch (AccessDeniedException | SQLException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void unmarkDiskAdding(Resource rsc)
    {
        try
        {
            rsc.getStateFlags().disableFlags(apiCtx, Resource.RscFlags.DISK_ADDING);
        }
        catch (AccessDeniedException | SQLException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void markDiskAdded(ResourceData rscData)
    {
        try
        {
            rscData.getStateFlags().disableFlags(apiCtx, Resource.RscFlags.DISKLESS);
            rscData.getStateFlags().disableFlags(apiCtx, Resource.RscFlags.DISK_ADDING);
            rscData.getStateFlags().disableFlags(apiCtx, Resource.RscFlags.DISK_ADD_REQUESTED);
        }
        catch (AccessDeniedException | SQLException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void setStorPool(Volume vlm, StorPool storPool)
    {
        try
        {
            vlm.setStorPool(apiCtx, storPool);
        }
        catch (AccessDeniedException | SQLException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private LockGuard createLockGuard()
    {
        return LockGuard.createDeferred(
            nodesMapLock.writeLock(),
            rscDfnMapLock.writeLock()
        );
    }

    private static Flux<ApiCallRc> translateDeploymentSuccess(
        Flux<Tuple2<NodeName, ApiCallRc>> responses,
        String messageFormat
    )
    {
        return responses.map(namedResponse ->
            {
                NodeName responseNodeName = namedResponse.getT1();
                ApiCallRc response = namedResponse.getT2();
                ApiCallRcImpl transformedResponses = new ApiCallRcImpl();
                for (ApiCallRc.RcEntry rcEntry : response.getEntries())
                {
                    if (rcEntry.getReturnCode() == ApiConsts.CREATED)
                    {
                        transformedResponses.addEntry(ApiCallRcImpl.simpleEntry(
                            ApiConsts.MODIFIED,
                            String.format(messageFormat, responseNodeName.displayValue)
                        ));
                    }
                    else
                    {
                        transformedResponses.addEntry(rcEntry);
                    }
                }
                return transformedResponses;
            }
        );
    }
}
