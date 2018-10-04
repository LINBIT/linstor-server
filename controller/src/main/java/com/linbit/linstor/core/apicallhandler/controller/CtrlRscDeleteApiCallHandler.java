package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
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
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.SwordfishInitiatorDriverKind;
import com.linbit.locks.LockGuard;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;
import static com.linbit.utils.StringUtils.firstLetterCaps;

@Singleton
public class CtrlRscDeleteApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlRscDeleteApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
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
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        peerAccCtx = peerAccCtxRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        Iterator<Resource> rscIter = rscDfn.iterateResource(apiCtx);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (!rsc.getAssignedNode().getFlags().isSet(apiCtx, Node.NodeFlag.DELETE) &&
                !rscDfn.getFlags().isSet(apiCtx, ResourceDefinition.RscDfnFlags.DELETE) &&
                rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DELETE))
            {
                fluxes.add(updateSatellites(rsc.getAssignedNode().getName(), rscDfn.getName()));
            }
        }

        return fluxes;
    }

    /**
     * Deletes a {@link Resource}.
     * <p>
     * The {@link Resource} is only deleted once the corresponding satellite confirmed
     * that it has undeployed (deleted) the {@link Resource}
     */
    public Flux<ApiCallRc> deleteResource(String nodeNameStr, String rscNameStr)
    {
        ResponseContext context = CtrlRscApiCallHandler.makeRscContext(
            ApiOperation.makeDeleteOperation(),
            nodeNameStr,
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                LockGuard.createDeferred(nodesMapLock.writeLock(), rscDfnMapLock.writeLock()),
                () -> deleteResourceInTransaction(nodeNameStr, rscNameStr)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteResourceInTransaction(String nodeNameStr, String rscNameStr)
    {
        NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, false);

        if (rsc == null)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_NOT_FOUND,
                getRscDescription(nodeNameStr, rscNameStr) + " not found."
            ));
        }

        SatelliteState stltState;
        Peer peer = getPeerPriveleged(rsc.getAssignedNode());
        Lock readLock = peer.getSatelliteStateLock().readLock();
        readLock.lock();
        try
        {
            stltState = peer.getSatelliteState();
        }
        finally
        {
            readLock.unlock();
        }
        SatelliteResourceState rscState = stltState == null ? null : stltState.getResourceStates().get(rscName);

        if (rscState != null && rscState.isInUse() != null && rscState.isInUse())
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_IN_USE,
                    String.format("Resource '%s' is still in use.", rscNameStr)
                )
                .setCause("Resource is mounted/in use.")
                .setCorrection(String.format("Un-mount resource '%s' on the node '%s'.", rscNameStr, nodeNameStr))
                .build()
            );
        }

        markDeletedWithVolumes(rsc);

        ctrlTransactionHelper.commit();

        String descriptionFirstLetterCaps = firstLetterCaps(getRscDescription(rsc));
        ApiCallRc responses = ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
            .entryBuilder(
                ApiConsts.DELETED,
                descriptionFirstLetterCaps + " marked for deletion."
            )
            .setDetails(descriptionFirstLetterCaps + " UUID is: " + rsc.getUuid())
            .build()
        );

        return Flux
            .just(responses)
            .concatWith(updateSatellites(nodeName, rscName));
    }

    // Restart from here when connection established and DELETE flag set
    private Flux<ApiCallRc> updateSatellites(NodeName nodeName, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                LockGuard.createDeferred(rscDfnMapLock.readLock()),
                () -> updateSatellitesInScope(nodeName, rscName)
            );
    }

    private Flux<ApiCallRc> updateSatellitesInScope(NodeName nodeName, ResourceName rscName)
    {
        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, false);

        Flux<ApiCallRc> flux;

        if (rsc == null)
        {
            flux = Flux.empty();
        }
        else
        {
            flux = ctrlSatelliteUpdateCaller.updateSatellites(rsc)
                .transform(updateResponses -> ResponseUtils.translateDeploymentSuccess(
                    updateResponses,
                    Collections.singleton(nodeName),
                    "Deleted {1} on {0}",
                    "Notified {0} that {1} is being deleted on ''" + nodeName + "''"
                ))
                .concatWith(deleteData(nodeName, rscName))
                .onErrorResume(CtrlSatelliteUpdateCaller.DelayedApiRcException.class, ignored -> Flux.empty());
        }

        return flux;
    }

    private Flux<ApiCallRc> deleteData(NodeName nodeName, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                LockGuard.createDeferred(rscDfnMapLock.writeLock()),
                () -> deleteDataInTransaction(nodeName, rscName)
            );
    }

    private Flux<ApiCallRc> deleteDataInTransaction(NodeName nodeName, ResourceName rscName)
    {
        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, false);

        Flux<ApiCallRc> flux;

        if (rsc == null)
        {
            flux = Flux.empty();
        }
        else
        {
            UUID rscUuid = rsc.getUuid();
            String descriptionFirstLetterCaps = firstLetterCaps(getRscDescription(rsc));
            ResourceDefinition rscDfn = rsc.getDefinition();

            checkForDependencies(rsc);

            deletePriveleged(rsc);

            if (rscDfn.getResourceCount() == 0)
            {
                // remove primary flag
                errorReporter.logDebug(
                    String.format("Resource definition '%s' empty, deleting primary flag.", rscName)
                );
                removePropPrimarySetPriveleged(rscDfn);
            }

            ctrlTransactionHelper.commit();

            flux = Flux.just(ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
                .entryBuilder(ApiConsts.DELETED, descriptionFirstLetterCaps + " deletion complete.")
                .setDetails(descriptionFirstLetterCaps + " UUID was: " + rscUuid)
                .build()
            ));
        }

        return flux;
    }

    /**
     * This method currently only checks if the current resource is a swordfish-target-resource and the
     * resource definition still has swordfish-initiator-resource(s).
     *
     * @param rsc
     */
    private void checkForDependencies(ResourceData rsc)
    {
        ResourceDefinition rscDfn = rsc.getDefinition();

        List<Volume> swordfishInitiatorVolumes = getSwordfishInitiatorVolume(rscDfn);
        if (
            isSwordfishTargetResource(rsc) &&
            !swordfishInitiatorVolumes.isEmpty()
        )
        {
            // swordfish currently might not allow multiple initiator per target, but nvme does
            List<String> nodeNames = swordfishInitiatorVolumes.stream()
                    .map(vlm -> vlm.getResource().getAssignedNode().getName().displayValue)
                    .collect(Collectors.toList());

            String rscNameStr = getRscDescription(rsc);
            String nodeNamesStr = nodeNames.size() == 1 ? nodeNames.get(0) : nodeNames.toString();
            throw new ApiRcException(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_IN_USE,
                    String.format("Swordfish-target resource '%s' is still in use.", rscNameStr)
                )
                .setCause("Resource is mounted/in use.")
                .setCorrection(
                    String.format(
                        "First remove the swordfish-initiator resource on node%s '%s'",
                        nodeNames.size() == 1 ? "" : "s",
                        nodeNamesStr
                    )
                )
                .build()
            );
        }
    }

    private List<Volume> getSwordfishInitiatorVolume(ResourceDefinition rscDfn)
    {
        return streamResourcesPriveleged(rscDfn)
            .flatMap(rsc -> rsc.streamVolumes())
            .filter(vlm -> getStorPoolPriveleged(vlm).getDriverKind() instanceof SwordfishInitiatorDriverKind)
            .collect(Collectors.toList());
    }

    private StorPool getStorPoolPriveleged(Volume vlm)
    {
        StorPool storPool;
        try
        {
            storPool = vlm.getStorPool(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return storPool;
    }

    private boolean isSwordfishTargetResource(ResourceData rsc)
    {
        return getNodeTypePriveleged(rsc).equals(NodeType.SWORDFISH_TARGET);
    }

    private Stream<Resource> streamResourcesPriveleged(ResourceDefinition rscDfn)
    {
        Stream<Resource> stream;
        try
        {
            stream = rscDfn.streamResource(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return stream;
    }

    private NodeType getNodeTypePriveleged(ResourceData rsc)
    {
        NodeType nodeType;
        try
        {
            nodeType = rsc.getAssignedNode().getNodeType(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return nodeType;
    }

    private Peer getPeerPriveleged(Node node)
    {
        Peer nodePeer;
        try
        {
            nodePeer = node.getPeer(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return nodePeer;
    }

    private void markDeletedWithVolumes(ResourceData rscData)
    {
        try
        {
            rscData.markDeleted(peerAccCtx.get());
            Iterator<Volume> volumesIterator = rscData.iterateVolumes();
            while (volumesIterator.hasNext())
            {
                Volume vlm = volumesIterator.next();
                vlm.markDeleted(peerAccCtx.get());
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getRscDescription(rscData) + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void deletePriveleged(Resource rsc)
    {
        try
        {
            rsc.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void removePropPrimarySetPriveleged(ResourceDefinition rscDfn)
    {
        try
        {
            rscDfn.getProps(apiCtx).removeProp(InternalApiConsts.PROP_PRIMARY_SET);
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

}
