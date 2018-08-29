package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlFullSyncApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlStltSerializer interComSerializer;
    private final Set<CtrlSatelliteConnectionListener> satelliteConnectionListeners;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;

    @Inject
    CtrlFullSyncApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlStltSerializer interComSerializerRef,
        Set<CtrlSatelliteConnectionListener> satelliteConnectionListenersRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        interComSerializer = interComSerializerRef;
        satelliteConnectionListeners = satelliteConnectionListenersRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
    }

    public Flux<?> sendFullSync(Peer satellite, long expectedFullSyncId)
    {
        return scopeRunner.fluxInTransactionlessScope(
            LockGuard.createDeferred(
                nodesMapLock.readLock(),
                rscDfnMapLock.readLock(),
                storPoolDfnMapLock.readLock(),
                satellite.getSerializerLock().writeLock()
            ),
            () -> sendFullSyncInScope(satellite, expectedFullSyncId)
        );
    }

    public Flux<?> fullSyncSuccess(Peer satellite)
    {
        return scopeRunner.fluxInTransactionlessScope(
            LockGuard.createDeferred(
                nodesMapLock.writeLock(),
                rscDfnMapLock.readLock()
            ),
            () -> fullSyncSuccessInScope(satellite)
        );
    }

    private Flux<?> sendFullSyncInScope(Peer satellite, long expectedFullSyncId)
    {
        try
        {
            Node localNode = satellite.getNode();

            Set<Node> nodes = new LinkedHashSet<>();
            Set<StorPool> storPools = new LinkedHashSet<>();
            Set<Resource> rscs = new LinkedHashSet<>();
            Set<Snapshot> snapshots = new LinkedHashSet<>();

            nodes.add(localNode); // always add the localNode

            for (Resource rsc : localNode.streamResources(apiCtx).collect(toList()))
            {
                rscs.add(rsc);
                Iterator<Resource> otherRscIterator = rsc.getDefinition().iterateResource(apiCtx);
                while (otherRscIterator.hasNext())
                {
                    Resource otherRsc = otherRscIterator.next();
                    if (otherRsc != rsc)
                    {
                        nodes.add(otherRsc.getAssignedNode());
                    }
                }
            }
            // some storPools might have been created on the satellite, but are not used by resources / volumes
            // however, when a rsc / vlm is created, they already assume the referenced storPool already exists
            storPools.addAll(localNode.streamStorPools(apiCtx).collect(toList()));

            snapshots.addAll(localNode.getInProgressSnapshots(apiCtx));

            satellite.setFullSyncId(expectedFullSyncId);

            errorReporter.logTrace("Sending full sync to " + satellite + ".");
            satellite.sendMessage(
                interComSerializer
                    .onewayBuilder(InternalApiConsts.API_FULL_SYNC_DATA)
                    .fullSync(nodes, storPools, rscs, snapshots, expectedFullSyncId, -1) // fullSync has -1 as updateId
                    .build()
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "ApiCtx does not have enough privileges to create a full sync for satellite " + satellite.getId(),
                    accDeniedExc
                )
            );
        }

        return Flux.empty();
    }

    private Flux<?> fullSyncSuccessInScope(Peer satellite)
    {
        satellite.setConnectionStatus(Peer.ConnectionStatus.ONLINE);

        Node localNode = satellite.getNode();

        List<Flux<?>> fluxes = new ArrayList<>();

        try
        {
            Iterator<Resource> localRscIter = localNode.iterateResources(apiCtx);
            while (localRscIter.hasNext())
            {
                Resource localRsc = localRscIter.next();
                ResourceDefinition rscDfn = localRsc.getDefinition();
                ResourceName rscName = rscDfn.getName();

                boolean allOnline = true;
                Iterator<Resource> rscIter = rscDfn.iterateResource(apiCtx);
                while (rscIter.hasNext())
                {
                    Resource rsc = rscIter.next();
                    if (rsc.getAssignedNode().getPeer(apiCtx).getConnectionStatus() != Peer.ConnectionStatus.ONLINE)
                    {
                        allOnline = false;
                        break;
                    }
                }

                if (allOnline)
                {
                    fluxes.add(notifyResourceDefinitionConnected(localNode, rscDfn));
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

        return Flux.merge(fluxes);
    }

    private Flux<?> notifyResourceDefinitionConnected(Node localNode, ResourceDefinition rscDfn)
    {
        List<Flux<ApiCallRc>> connectionListenerResponses = new ArrayList<>();

        for (CtrlSatelliteConnectionListener connectionListener : satelliteConnectionListeners)
        {
            try
            {
                connectionListenerResponses.add(
                    connectionListener.resourceDefinitionConnected(rscDfn)
                );
            }
            catch (Exception | ImplementationError exc)
            {
                errorReporter.reportError(
                    exc,
                    null,
                    null,
                    "Error performing operations after connecting to " + localNode
                );
            }
        }

        return Flux.fromIterable(connectionListenerResponses)
            .flatMap(listenerFlux -> listenerFlux.onErrorResume(exc -> handleListenerError(localNode, exc)));
    }

    private Publisher<? extends ApiCallRc> handleListenerError(Node localNode, Throwable exc)
    {
        errorReporter.reportError(
            exc,
            null,
            null,
            "Error emitted performing operations after connecting to " + localNode
        );
        return Flux.empty();
    }
}
