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
import com.linbit.linstor.core.LinStor;
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
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
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
import java.util.concurrent.locks.ReadWriteLock;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.makeRscContext;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescription;

/**
 * Adds disks to a diskless resource or removes disks to make a resource diskless.
 * <p>
 * When adding disks to a diskless resource, the states defined by the following flags are used:
 * <ol>
 *     <li>DISKLESS, DISK_ADD_REQUESTED - the transition has been requested but not yet started</li>
 *     <li>DISKLESS, DISK_ADD_REQUESTED, DISK_ADDING - the peers should prepare for the resource to gain disks</li>
 *     <li>none - the disks should be added</li>
 * </ol>
 * <p>
 * When removing disks to make a resource diskless, the states defined by the following flags are used:
 * <ol>
 *     <li>DISK_REMOVE_REQUESTED - the transition has been requested but not yet started</li>
 *     <li>DISKLESS, DISK_REMOVE_REQUESTED, DISK_REMOVING - the disks should be removed</li>
 *     <li>DISKLESS - the peers should acknowledge the removal of the disks</li>
 * </ol>
 */
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
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        ResourceName rscName = rscDfn.getName();

        Iterator<Resource> rscIter = rscDfn.iterateResource(apiCtx);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            boolean diskAddRequested =
                rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISK_ADD_REQUESTED);
            boolean diskRemoveRequested =
                rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISK_REMOVE_REQUESTED);
            if (diskAddRequested || diskRemoveRequested)
            {
                NodeName nodeName = rsc.getAssignedNode().getName();
                fluxes.add(updateAndAdjustDisk(nodeName, rscName, diskRemoveRequested));
            }
        }

        return fluxes;
    }

    public Flux<ApiCallRc> resourceToggleDisk(
        String nodeNameStr,
        String rscNameStr,
        String storPoolNameStr,
        boolean removeDisk
    )
    {
        ResponseContext context = makeRscContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                createLockGuard(),
                () -> toggleDiskInTransaction(nodeNameStr, rscNameStr, storPoolNameStr, removeDisk)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> toggleDiskInTransaction(
        String nodeNameStr,
        String rscNameStr,
        String storPoolNameStr,
        boolean removeDisk
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);

        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, true);

        if (hasDiskAddRequested(rsc))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_RSC_BUSY,
                "Additional of disk to resource already requested"
            ));
        }
        if (hasDiskRemoveRequested(rsc))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_RSC_BUSY,
                "Removal of disk from resource already requested"
            ));
        }

        if (!removeDisk && !ctrlVlmCrtApiHelper.isDiskless(rsc))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_RSC_ALREADY_HAS_DISK,
                "Resource already has disk"
            ));
        }
        if (removeDisk && ctrlVlmCrtApiHelper.isDiskless(rsc))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_RSC_ALREADY_DISKLESS,
                "Resource already diskless"
            ));
        }

        if (removeDisk)
        {
            // Prevent removal of the last disk
            int haveDiskCount = countDisks(rsc.getDefinition());
            if (haveDiskCount <= 1)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INSUFFICIENT_REPLICA_COUNT,
                    "Cannot remove the disk from the only resource with a disk"
                ));
            }
        }

        // Save the requested storage pool in the resource properties.
        // This does not cause the storage pool to be used automatically.
        Props rscProps = ctrlPropsHelper.getProps(rsc);
        if (storPoolNameStr == null || storPoolNameStr.isEmpty())
        {
            if (removeDisk)
            {
                rscProps.map().put(ApiConsts.KEY_STOR_POOL_NAME, LinStor.DISKLESS_STOR_POOL_NAME);
            }
            else
            {
                rscProps.map().remove(ApiConsts.KEY_STOR_POOL_NAME);
            }
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

            ctrlVlmCrtApiHelper.resolveStorPool(rsc, vlmDfn, removeDisk).extractApiCallRc(responses);
        }

        if (removeDisk)
        {
            markDiskRemoveRequested(rsc);
        }
        else
        {
            markDiskAddRequested(rsc);
        }

        ctrlTransactionHelper.commit();

        String action = removeDisk ? "Removal of disk from" : "Addition of disk to";
        responses.addEntry(ApiCallRcImpl.simpleEntry(
            ApiConsts.MODIFIED,
            action + " resource '" + rsc.getDefinition().getName().displayValue + "' " +
                "on node '" + rsc.getAssignedNode().getName().displayValue + "' registered"
        ));

        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(updateAndAdjustDisk(nodeName, rscName, removeDisk));
    }

    // Restart from here when connection established and flag set
    private Flux<ApiCallRc> updateAndAdjustDisk(NodeName nodeName, ResourceName rscName, boolean removeDisk)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                createLockGuard(),
                () -> updateAndAdjustDiskInTransaction(nodeName, rscName, removeDisk)
            );
    }

    private Flux<ApiCallRc> updateAndAdjustDiskInTransaction(
        NodeName nodeName,
        ResourceName rscName,
        boolean removeDisk
    )
    {
        Flux<ApiCallRc> responses;

        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, true);

        ApiCallRcImpl offlineWarnings = new ApiCallRcImpl();

        try
        {
            Iterator<Resource> rscIterator = rsc.getDefinition().iterateResource(apiCtx);
            while (rscIterator.hasNext())
            {
                Resource currentRsc = rscIterator.next();
                Node node = currentRsc.getAssignedNode();
                if (node.getPeer(apiCtx).getConnectionStatus() != Peer.ConnectionStatus.ONLINE)
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
            responses = Flux.just(offlineWarnings);
        }
        else
        {
            if (removeDisk)
            {
                markDiskRemoving(rsc);
            }
            else
            {
                markDiskAdding(rsc);
            }
            ctrlTransactionHelper.commit();

            String actionSelf = removeDisk ? "Removed disk on {0}" : null;
            String actionPeer = removeDisk ? null : "Prepared {0} to expect disk on ''" + nodeName.displayValue + "''";
            Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(rsc)
                .transform(updateResponses -> ResponseUtils.translateDeploymentSuccess(
                    updateResponses,
                    nodeName,
                    actionSelf,
                    actionPeer
                ));

            responses = satelliteUpdateResponses
                // If an update fails (e.g. the connection to a node is lost), attempt to reset back to the
                // initial state. The requested flag is not reset, so the operation will be retried when the
                // nodes are next all connected.
                // There is no point attempting to reset a disk removal because the underlying storage volume
                // may have been removed.
                .transform(flux -> removeDisk ? flux :
                    flux
                        .onErrorResume(error ->
                            resetDiskAdding(nodeName, rscName)
                                .concatWith(Flux.error(error))
                        )
                )
                .concatWith(finishOperation(nodeName, rscName, removeDisk))
                .onErrorResume(CtrlSatelliteUpdateCaller.DelayedApiRcException.class, ignored -> Flux.empty());
        }

        return responses;
    }

    private Flux<ApiCallRc> resetDiskAdding(NodeName nodeName, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                createLockGuard(),
                () -> resetDiskAddingInTransaction(nodeName, rscName)
            );
    }

    private Flux<ApiCallRc> resetDiskAddingInTransaction(
        NodeName nodeName,
        ResourceName rscName
    )
    {
        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, true);

        unmarkDiskAdding(rsc);

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(rsc)
            .transform(responses -> ResponseUtils.translateDeploymentSuccess(
                responses,
                "Diskless state temporarily reset on {0}"
            ));

        return satelliteUpdateResponses
            .onErrorResume(CtrlSatelliteUpdateCaller.DelayedApiRcException.class, ignored -> Flux.empty());
    }

    private Flux<ApiCallRc> finishOperation(NodeName nodeName, ResourceName rscName, boolean removeDisk)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                createLockGuard(),
                () -> finishOperationInTransaction(nodeName, rscName, removeDisk)
            );
    }

    private Flux<ApiCallRc> finishOperationInTransaction(
        NodeName nodeName,
        ResourceName rscName,
        boolean removeDisk
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, true);

        if (removeDisk)
        {
            markDiskRemoved(rsc);
        }
        else
        {
            markDiskAdded(rsc);
        }

        Iterator<Volume> vlmIter = rsc.iterateVolumes();
        while (vlmIter.hasNext())
        {
            Volume vlm = vlmIter.next();

            StorPool storPool = ctrlVlmCrtApiHelper.resolveStorPool(rsc, vlm.getVolumeDefinition(), removeDisk)
                .extractApiCallRc(responses);

            setStorPool(vlm, storPool);
        }

        ctrlTransactionHelper.commit();

        String actionSelf = removeDisk ? null : "Added disk on {0}";
        String actionPeer = removeDisk ?
            "Notified {0} that disk has been removed on ''" + nodeName.displayValue + "''" : null;
        Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(rsc)
            .transform(updateResponses -> ResponseUtils.translateDeploymentSuccess(
                updateResponses,
                nodeName,
                actionSelf,
                actionPeer
            ));

        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(satelliteUpdateResponses);
    }

    private int countDisks(ResourceDefinition rscDfn)
    {
        int haveDiskCount = 0;
        try
        {
            Iterator<Resource> rscIter = rscDfn.iterateResource(peerAccCtx.get());
            while (rscIter.hasNext())
            {
                Resource rsc = rscIter.next();
                if (!ctrlVlmCrtApiHelper.isDiskless(rsc))
                {
                    haveDiskCount++;
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "count disks in " + getRscDfnDescription(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return haveDiskCount;
    }

    private boolean hasDiskAddRequested(Resource rsc)
    {
        boolean set;
        try
        {
            set = rsc.getStateFlags().isSet(peerAccCtx.get(), Resource.RscFlags.DISK_ADD_REQUESTED);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check whether addition of disk requested for " + getRscDescription(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return set;
    }

    private boolean hasDiskRemoveRequested(Resource rsc)
    {
        boolean set;
        try
        {
            set = rsc.getStateFlags().isSet(peerAccCtx.get(), Resource.RscFlags.DISK_REMOVE_REQUESTED);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check whether removal of disk requested for " + getRscDescription(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return set;
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

    private void markDiskRemoveRequested(Resource rsc)
    {
        try
        {
            rsc.getStateFlags().enableFlags(peerAccCtx.get(), Resource.RscFlags.DISK_REMOVE_REQUESTED);
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

    private boolean hasDiskRemoving(Resource rsc)
    {
        boolean diskAdding;
        try
        {
            diskAdding = rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISK_REMOVING);
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

    private void markDiskRemoving(Resource rsc)
    {
        try
        {
            rsc.getStateFlags().enableFlags(
                apiCtx,
                Resource.RscFlags.DISKLESS,
                Resource.RscFlags.DISK_REMOVING
            );
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
            rscData.getStateFlags().disableFlags(
                apiCtx,
                Resource.RscFlags.DISKLESS,
                Resource.RscFlags.DISK_ADDING,
                Resource.RscFlags.DISK_ADD_REQUESTED
            );
        }
        catch (AccessDeniedException | SQLException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void markDiskRemoved(ResourceData rscData)
    {
        try
        {
            rscData.getStateFlags().disableFlags(
                apiCtx,
                Resource.RscFlags.DISK_REMOVING,
                Resource.RscFlags.DISK_REMOVE_REQUESTED
            );
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
}
