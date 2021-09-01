package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSatelliteConnectionNotifier;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlFullSyncResponseApiCallHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlSatelliteConnectionNotifier ctrlSatelliteConnectionNotifier;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final Provider<Peer> satelliteProvider;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapDelApiCallHandler;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlTransactionHelper ctrlTransactionHelper;

    @Inject
    public CtrlFullSyncResponseApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlSatelliteConnectionNotifier ctrlSatelliteConnectionNotifierRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        Provider<Peer> satelliteProviderRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDelApiCallHandlerRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlTransactionHelper ctrlTransactionHelperRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlSatelliteConnectionNotifier = ctrlSatelliteConnectionNotifierRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        satelliteProvider = satelliteProviderRef;
        ctrlSnapDelApiCallHandler = ctrlSnapDelApiCallHandlerRef;
        backupInfoMgr = backupInfoMgrRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
    }

    public Flux<?> fullSyncSuccess()
    {
        return fullSyncSuccess(satelliteProvider.get(), null);
    }

    public Flux<?> fullSyncSuccess(Peer satellitePeerRef, ResponseContext context)
    {
        final ResponseContext ctx;
        if (context == null)
        {
            ctx = CtrlNodeApiCallHandler.makeNodeContext(
                ApiOperation.makeCreateOperation(),
                satellitePeerRef.getNode().getName().displayValue
            );
        }
        else
        {
            ctx = context;
        }
        return scopeRunner.fluxInTransactionalScope(
            "Handle full sync success",
            LockGuard.createDeferred(
                nodesMapLock.writeLock(),
                rscDfnMapLock.readLock()
            ),
            () -> fullSyncSuccessInScope(satellitePeerRef, ctx)
        );
    }

    private Flux<?> fullSyncSuccessInScope(Peer satellitePeerRef, ResponseContext context)
    {
        satellitePeerRef.setConnectionStatus(ApiConsts.ConnectionStatus.ONLINE);
        satellitePeerRef.fullSyncApplied();

        Node localNode = satellitePeerRef.getNode();

        List<Flux<?>> fluxes = new ArrayList<>();

        try
        {
            Iterator<Resource> localRscIter = localNode.iterateResources(apiCtx);
            while (localRscIter.hasNext())
            {
                Resource localRsc = localRscIter.next();
                fluxes.add(ctrlSatelliteConnectionNotifier.resourceConnected(localRsc, context));
            }

            Iterator<Snapshot> localSnapIter = localNode.iterateSnapshots(apiCtx);
            while (localSnapIter.hasNext())
            {
                Snapshot localSnap = localSnapIter.next();
                if (
                    localSnap.getFlags().isSet(apiCtx, Snapshot.Flags.BACKUP_TARGET) &&
                    localSnap.getSnapshotDefinition().getFlags().isSet(
                        apiCtx,
                        SnapshotDefinition.Flags.SHIPPING,
                        SnapshotDefinition.Flags.BACKUP
                    )
                )
                {
                    // complete abort, remove restore-lock on rscDfn
                    backupInfoMgr.restoreRemoveEntry(localSnap.getResourceDefinition());
                    fluxes.add(
                        ctrlSnapDelApiCallHandler.deleteSnapshot(
                            localSnap.getResourceName().displayValue,
                            localSnap.getSnapshotName().displayValue,
                            null
                        )
                    );
                }
            }
            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

        return Flux.merge(fluxes);
    }

    public Flux<?> fullSyncFailed(Peer satellitePeerRef)
    {
        return scopeRunner.fluxInTransactionlessScope(
            "Handle full sync failed",
            LockGuard.createDeferred(
                nodesMapLock.writeLock(),
                rscDfnMapLock.readLock()
            ),
            () -> fullSyncFailedInScope(satellitePeerRef)
        );
    }

    private Flux<?> fullSyncFailedInScope(Peer satellitePeerRef)
    {
        satellitePeerRef.fullSyncFailed();
        return Flux.empty();
    }
}
