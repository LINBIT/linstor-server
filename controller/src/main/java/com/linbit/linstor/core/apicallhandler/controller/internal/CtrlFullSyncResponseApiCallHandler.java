package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSatelliteConnectionNotifier;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
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

    @Inject
    public CtrlFullSyncResponseApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlSatelliteConnectionNotifier ctrlSatelliteConnectionNotifierRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        Provider<Peer> satelliteProviderRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlSatelliteConnectionNotifier = ctrlSatelliteConnectionNotifierRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        satelliteProvider = satelliteProviderRef;
    }

    public Flux<?> fullSyncSuccess()
    {
        return fullSyncSuccess(satelliteProvider.get());
    }

    public Flux<?> fullSyncSuccess(Peer satellitePeerRef)
    {
        return scopeRunner.fluxInTransactionlessScope(
            "Handle full sync success",
            LockGuard.createDeferred(
                nodesMapLock.writeLock(),
                rscDfnMapLock.readLock()
            ),
            () -> fullSyncSuccessInScope(satellitePeerRef)
        );
    }

    private Flux<?> fullSyncSuccessInScope(Peer satellitePeerRef)
    {
        satellitePeerRef.setConnectionStatus(Peer.ConnectionStatus.ONLINE);

        Node localNode = satellitePeerRef.getNode();

        List<Flux<?>> fluxes = new ArrayList<>();

        try
        {
            Iterator<Resource> localRscIter = localNode.iterateResources(apiCtx);
            while (localRscIter.hasNext())
            {
                Resource localRsc = localRscIter.next();
                fluxes.add(ctrlSatelliteConnectionNotifier.resourceConnected(localRsc));
            }
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
