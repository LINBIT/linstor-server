package com.linbit.linstor.core.apicallhandler.controller.internal;

import static java.util.stream.Collectors.toList;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer.CtrlStltSerializerBuilder;
import com.linbit.linstor.api.protobuf.internal.IntFullSyncResponse;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.locks.LockGuard;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlFullSyncApiCallHandler
{
    private static final Long FULL_SYNC_RPC_ID = -1L;

    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlStltSerializer interComSerializer;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final IntFullSyncResponse fullSyncResponse;

    @Inject
    CtrlFullSyncApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlStltSerializer interComSerializerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        IntFullSyncResponse fullSyncResponseRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        interComSerializer = interComSerializerRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        fullSyncResponse = fullSyncResponseRef;
    }

    public Flux<?> sendFullSync(Node satelliteNode, long expectedFullSyncId)
    {
        return sendFullSync(satelliteNode, expectedFullSyncId, false);
    }

    public Flux<ApiCallRc> sendFullSync(Node satelliteNode, long expectedFullSyncId, boolean waitForAnswer)
    {
        Peer peer;
        try
        {
            peer = satelliteNode.getPeer(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return scopeRunner.fluxInTransactionlessScope(
            "Send full sync",
            LockGuard.createDeferred(
                nodesMapLock.readLock(),
                rscDfnMapLock.readLock(),
                storPoolDfnMapLock.readLock(),
                peer.getSerializerLock().writeLock()
            ),
            () -> sendFullSyncInScope(satelliteNode, expectedFullSyncId, waitForAnswer)
        );
    }

    private Flux<ApiCallRc> sendFullSyncInScope(Node satelliteNode, long expectedFullSyncId, boolean waitForAnswer)
    {
        Flux<ApiCallRc> flux = Flux.empty();
        try
        {
            Set<Node> nodes = new LinkedHashSet<>();
            Set<StorPool> storPools = new LinkedHashSet<>();
            Set<Resource> rscs = new LinkedHashSet<>();
            Set<Snapshot> snapshots = new LinkedHashSet<>();

            nodes.add(satelliteNode); // always add the localNode

            for (Resource rsc : satelliteNode.streamResources(apiCtx).collect(toList()))
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
            storPools.addAll(satelliteNode.streamStorPools(apiCtx).collect(toList()));

            snapshots.addAll(satelliteNode.getInProgressSnapshots(apiCtx));

            Peer satellitePeer = satelliteNode.getPeer(apiCtx);
            satellitePeer.setFullSyncId(expectedFullSyncId);

            errorReporter.logTrace("Sending full sync to " + satelliteNode + ".");

            CtrlStltSerializerBuilder builder;
            if (waitForAnswer)
            {
                builder = interComSerializer.headerlessBuilder();
            }
            else
            {
                builder = interComSerializer.apiCallBuilder(
                    InternalApiConsts.API_FULL_SYNC_DATA,
                    FULL_SYNC_RPC_ID
                );
            }

            byte[] data = builder
                .fullSync(nodes, storPools, rscs, snapshots, expectedFullSyncId, FULL_SYNC_RPC_ID)
                .build();

            if (waitForAnswer)
            {
                StringBuilder details = new StringBuilder();
                ExtToolsManager extToolsManager = satellitePeer.getExtToolsManager();
                Map<DeviceLayerKind, List<String>> unsupportedLayersWithResons =
                    extToolsManager.getUnsupportedLayersWithResons();
                Map<DeviceProviderKind, List<String>> unsupportedProvidersWithResons =
                    extToolsManager.getUnsupportedProvidersWithResons();

                details.append("Supported storage providers: ")
                    .append(extToolsManager.getSupportedProviders().toString().toLowerCase())
                    .append("\nSupported resource layers  : ")
                    .append(extToolsManager.getSupportedLayers().toString().toLowerCase());

                renderUnsupportedDetails(details, unsupportedProvidersWithResons, "storage providers");
                renderUnsupportedDetails(details, unsupportedLayersWithResons, "resource layers");

                flux = satellitePeer.apiCall(
                        InternalApiConsts.API_FULL_SYNC_DATA,
                        data
                    )
                    .concatMap(inputStream -> handleFullSyncResponse(satellitePeer, inputStream))
                    .thenMany(
                        Flux.just(
                            ApiCallRcImpl.singletonApiCallRc(
                                ApiCallRcImpl.simpleEntry(
                                    ApiConsts.CONN_STATUS_AUTHENTICATED,
                                    "Node '" + satelliteNode.getName().displayValue + "' authenticated"
                                )
                                .setDetails(details.toString())
                            )
                        )
                    );
            }
            else
            {
                satellitePeer.sendMessage(data);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "ApiCtx does not have enough privileges to create a full sync for satellite " +
                        satelliteNode.getName(),
                    accDeniedExc
                )
            );
        }

        return flux;
    }

    private Flux<byte[]> handleFullSyncResponse(Peer satellitePeerRef, InputStream inputStream)
    {
        Flux<byte[]> flux;
        try
        {
            flux = fullSyncResponse.processReactive(satellitePeerRef, inputStream);
        }
        catch (IOException exc)
        {
            flux = Flux.error(exc);
        }
        return flux;
    }

    private <T extends Enum<T>> void renderUnsupportedDetails(
        StringBuilder details,
        Map<T, List<String>> unsupportedTypeWithReasons,
        String type
    )
    {
        if (!unsupportedTypeWithReasons.isEmpty())
        {
            details.append("\nUnsupported ").append(type).append(":\n");
            for (Entry<T, List<String>> entry : unsupportedTypeWithReasons.entrySet())
            {
                details.append("    ").append(entry.getKey()).append(": ");
                String indent = StringUtils.repeat(" ", "", 4 + 2 + entry.getKey().name().length());

                boolean first = true;
                for (String reason : entry.getValue())
                {
                    if (first)
                    {
                        first = false;
                    }
                    else
                    {
                        details.append(indent);
                    }
                    details.append(reason).append("\n");
                }
            }
        }
    }
}
