package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.CtrlLayerStackHelper;
import com.linbit.linstor.CtrlStorPoolResolveHelper;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.BackgroundRunner;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.event.EventWaiter;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
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
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final BackgroundRunner backgroundRunner;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlVlmCrtApiHelper ctrlVlmCrtApiHelper;
    private final CtrlStorPoolResolveHelper ctrlStorPoolResolveHelper;
    private final CtrlRscDeleteApiHelper ctrlRscDeleteApiHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlLayerStackHelper ctrlLayerStackHelper;
    private final ResponseConverter responseConverter;
    private final ResourceStateEvent resourceStateEvent;
    private final EventWaiter eventWaiter;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlRscToggleDiskApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        BackgroundRunner backgroundRunnerRef, CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlVlmCrtApiHelper ctrlVlmCrtApiHelperRef,
        CtrlStorPoolResolveHelper ctrlStorPoolResolveHelperRef,
        CtrlRscDeleteApiHelper ctrlRscDeleteApiHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlLayerStackHelper ctrlLayerStackHelperRef,
        ResponseConverter responseConverterRef,
        ResourceStateEvent resourceStateEventRef,
        EventWaiter eventWaiterRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        backgroundRunner = backgroundRunnerRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlVlmCrtApiHelper = ctrlVlmCrtApiHelperRef;
        ctrlStorPoolResolveHelper = ctrlStorPoolResolveHelperRef;
        ctrlRscDeleteApiHelper = ctrlRscDeleteApiHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        ctrlLayerStackHelper = ctrlLayerStackHelperRef;
        responseConverter = responseConverterRef;
        resourceStateEvent = resourceStateEventRef;
        eventWaiter = eventWaiterRef;
        lockGuardFactory = lockGuardFactoryRef;
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

    @Override
    public Collection<Flux<ApiCallRc>> resourceConnected(Resource rsc)
        throws AccessDeniedException
    {
        String migrateFromNodeNameStr = getPropsPrivileged(rsc).map().get(ApiConsts.KEY_RSC_MIGRATE_FROM);

        // Only restart the migration watch if adding the disk is complete
        boolean diskAddRequested = rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISK_ADD_REQUESTED);

        return migrateFromNodeNameStr == null && !diskAddRequested ?
            Collections.emptySet() :
            Collections.singleton(Flux.from(waitForMigration(
                rsc.getAssignedNode().getName(),
                rsc.getDefinition().getName(),
                ctrlApiDataLoader.loadNode(migrateFromNodeNameStr, true).getName()
            )));
    }

    public Flux<ApiCallRc> resourceToggleDisk(
        String nodeNameStr,
        String rscNameStr,
        String storPoolNameStr,
        String migrateFromNodeNameStr,
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
                "Toggle disk",
                createLockGuard(),
                () -> toggleDiskInTransaction(
                    nodeNameStr,
                    rscNameStr,
                    storPoolNameStr,
                    migrateFromNodeNameStr,
                    removeDisk
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> toggleDiskInTransaction(
        String nodeNameStr,
        String rscNameStr,
        String storPoolNameStr,
        String migrateFromNodeNameStr,
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
                "Addition of disk to resource already requested"
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
            Volume vlm = vlmIter.next();
            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();

            StorPool storPool = ctrlStorPoolResolveHelper.resolveStorPool(rsc, vlmDfn, removeDisk)
                .extractApiCallRc(responses);

            if (!removeDisk)
            {
                setStorPool(vlm, storPool);
                removeStorageLayerData(rsc);
                ctrlLayerStackHelper.ensureStackDataExists(rsc, null, null);
            }
            // else (if we are removing disk) we first have to remove the actual disk. that means we can only update the storage layer
            // when the deviceManager already got rid of the actual volume(s)
        }

        if (removeDisk)
        {
            markDiskRemoveRequested(rsc);
        }
        else
        {
            markDiskAddRequested(rsc);

            if (migrateFromNodeNameStr != null && !migrateFromNodeNameStr.isEmpty())
            {
                Resource migrateFromRsc = ctrlApiDataLoader.loadRsc(migrateFromNodeNameStr, rscNameStr, true);

                ensureNoSnapshots(migrateFromRsc);

                setMigrateFrom(rsc, migrateFromRsc.getAssignedNode().getName());

                ctrlRscDeleteApiHelper.ensureNotInUse(migrateFromRsc);
            }
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
                "Update for disk toggle",
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
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    updateResponses,
                    rscName,
                    Collections.singleton(nodeName),
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
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
        }

        return responses;
    }

    private Flux<ApiCallRc> resetDiskAdding(NodeName nodeName, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Reset disk adding",
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
            .transform(responses -> CtrlResponseUtils.combineResponses(
                responses,
                rscName,
                "Diskless state temporarily reset on {0}"
            ));

        return satelliteUpdateResponses
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
    }

    private Flux<ApiCallRc> finishOperation(NodeName nodeName, ResourceName rscName, boolean removeDisk)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Finish disk toggle",
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

            StorPool storPool = ctrlStorPoolResolveHelper.resolveStorPool(rsc, vlm.getVolumeDefinition(), removeDisk)
                .extractApiCallRc(responses);

            setStorPool(vlm, storPool);
            if (removeDisk)
            {
                removeStorageLayerData(rsc);
                ctrlLayerStackHelper.ensureStackDataExists(rsc, null, null);
            }
        }

        ctrlTransactionHelper.commit();

        String actionSelf = removeDisk ? null : "Added disk on {0}";
        String actionPeer = removeDisk ?
            "Notified {0} that disk has been removed on ''" + nodeName.displayValue + "''" : null;
        Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(rsc)
            .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                updateResponses,
                rscName,
                Collections.singleton(nodeName),
                actionSelf,
                actionPeer
            ));

        Publisher<ApiCallRc> migrationFlux;
        Props rscProps = getPropsPrivileged(rsc);
        String migrateFromNodeNameStr = rscProps.map().get(ApiConsts.KEY_RSC_MIGRATE_FROM);
        if (migrateFromNodeNameStr == null)
        {
            migrationFlux = Flux.empty();
        }
        else
        {
            migrationFlux = waitForMigration(
                nodeName,
                rscName,
                ctrlApiDataLoader.loadNode(migrateFromNodeNameStr, true).getName()
            );
        }

        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(satelliteUpdateResponses)
            .concatWith(migrationFlux);
    }

    private Publisher<ApiCallRc> waitForMigration(
        NodeName nodeName,
        ResourceName rscName,
        NodeName migrateFromNodeName
    )
    {
        return Mono.fromRunnable(() -> backgroundRunner.runInBackground(
            "Migrate '" + rscName + "' from '" + migrateFromNodeName + "' to '" + nodeName + "'",
            eventWaiter
                .waitForStream(
                    resourceStateEvent.get(),
                    ObjectIdentifier.resource(nodeName, rscName)
                )
                .skipUntil(usageState -> usageState.getUpToDate() != null && usageState.getUpToDate())
                .next()
                .thenMany(scopeRunner.fluxInTransactionalScope(
                    "Delete after migrate",
                    lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.RSC_DFN_MAP),
                    () -> startDeletionInTransaction(nodeName, rscName, migrateFromNodeName)
                ))
                .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty())
        ));
    }

    private Flux<ApiCallRc> startDeletionInTransaction(
        NodeName nodeName,
        ResourceName rscName,
        NodeName migrateFromNodeName
    )
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, true);
        Resource migrateFromRsc = ctrlApiDataLoader.loadRsc(migrateFromNodeName, rscName, false);

        getPropsPrivileged(rsc).map().remove(ApiConsts.KEY_RSC_MIGRATE_FROM);

        Flux<ApiCallRc> deleteFlux;
        if (migrateFromRsc == null)
        {
            deleteFlux = Flux.empty();
        }
        else
        {
            ctrlRscDeleteApiHelper.markDeletedWithVolumes(migrateFromRsc);
            deleteFlux = ctrlRscDeleteApiHelper.updateSatellitesForResourceDelete(migrateFromNodeName, rscName);
        }

        ctrlTransactionHelper.commit();

        return deleteFlux;
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

    private void removeStorageLayerData(ResourceData rscRef)
    {
        try
        {
            List<RscLayerObject> storageDataList = LayerUtils.getChildLayerDataByKind(
                rscRef.getLayerData(peerAccCtx.get()),
                DeviceLayerKind.STORAGE
            );
            for (RscLayerObject rscLayerObject : storageDataList)
            {
                List<VlmProviderObject> vlmDataList = new ArrayList<>(rscLayerObject.getVlmLayerObjects().values());
                for (VlmProviderObject vlmData : vlmDataList)
                {
                    rscLayerObject.remove(vlmData.getVlmNr());
                }
            }

        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "update the storage layer data of " + getRscDescription(rscRef),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void ensureNoSnapshots(Resource rsc)
    {
        try
        {
            for (SnapshotDefinition snapshotDfn : rsc.getDefinition().getSnapshotDfns(peerAccCtx.get()))
            {
                Snapshot snapshot = snapshotDfn.getSnapshot(peerAccCtx.get(), rsc.getAssignedNode().getName());
                if (snapshot != null)
                {
                    throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_EXISTS_SNAPSHOT,
                        "Cannot migrate '" + rsc.getDefinition().getName() + "' " +
                            "from '" + rsc.getAssignedNode().getName() + "' because snapshots are present " +
                            "and snapshots cannot be migrated"
                    ));
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check for snapshots of " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
    }

    private void setMigrateFrom(ResourceData rsc, NodeName migrateFromNodeName)
    {
        try
        {
            rsc.getProps(peerAccCtx.get()).map().put(ApiConsts.KEY_RSC_MIGRATE_FROM, migrateFromNodeName.value);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "set migration source for " + getRscDescription(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
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

    private Props getPropsPrivileged(Resource rsc)
    {
        Props props;
        try
        {
            props = rsc.getProps(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return props;
    }

    private LockGuard createLockGuard()
    {
        return lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP);
    }
}
