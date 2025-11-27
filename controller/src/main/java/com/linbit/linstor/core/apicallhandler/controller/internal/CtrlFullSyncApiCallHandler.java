package com.linbit.linstor.core.apicallhandler.controller.internal;

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
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.repository.ExternalFileRepository;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnectorPeer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
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

import org.slf4j.MDC;
import reactor.core.publisher.Flux;

import static java.util.stream.Collectors.toList;

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
    private final ReadWriteLock externalFilesMapLock;
    private final ReadWriteLock remoteMapLock;
    private final RemoteRepository remoteRepo;
    private final IntFullSyncResponse fullSyncResponse;
    private final ExternalFileRepository externalFilesRepo;

    @Inject
    CtrlFullSyncApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlStltSerializer interComSerializerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(CoreModule.EXT_FILE_MAP_LOCK) ReadWriteLock externalFilesMapLockRef,
        @Named(CoreModule.REMOTE_MAP_LOCK) ReadWriteLock remoteMapLockRef,
        IntFullSyncResponse fullSyncResponseRef,
        ExternalFileRepository externalFilesRepoRef,
        RemoteRepository remoteRepoRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        interComSerializer = interComSerializerRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        externalFilesMapLock = externalFilesMapLockRef;
        remoteMapLock = remoteMapLockRef;
        remoteRepo = remoteRepoRef;
        fullSyncResponse = fullSyncResponseRef;
        externalFilesRepo = externalFilesRepoRef;
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
                externalFilesMapLock.readLock(),
                remoteMapLock.readLock(),
                peer.getSerializerLock().writeLock()
            ),
            () -> sendFullSyncInScope(satelliteNode, expectedFullSyncId, waitForAnswer),
            MDC.getCopyOfContextMap()
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
            Set<ExternalFile> externalFiles = new LinkedHashSet<>();
            Set<AbsRemote> remotes = new LinkedHashSet<>();

            nodes.add(satelliteNode); // always add the localNode

            // some storPools might have been created on the satellite, but are not used by resources / volumes
            // however, when a rsc / vlm is created, they already assume the referenced storPool already exists
            storPools.addAll(satelliteNode.streamStorPools(apiCtx).collect(toList()));

            for (Resource rsc : satelliteNode.streamResources(apiCtx).collect(toList()))
            {
                rscs.add(rsc);
                Iterator<Resource> otherRscIterator = rsc.getResourceDefinition().iterateResource(apiCtx);
                while (otherRscIterator.hasNext())
                {
                    Resource otherRsc = otherRscIterator.next();
                    if (otherRsc != rsc)
                    {
                        nodes.add(otherRsc.getNode());
                        storPools.addAll(LayerVlmUtils.getStorPools(otherRsc, apiCtx));
                    }
                }
            }

            // we need to send all snaps, since the stlt might need them for e.g. incremental backup shipping
            snapshots.addAll(satelliteNode.getSnapshots(apiCtx));

            externalFiles.addAll(externalFilesRepo.getMapForView(apiCtx).values());
            remotes.addAll(remoteRepo.getMapForView(apiCtx).values());

            Peer satellitePeer = satelliteNode.getPeer(apiCtx);
            satellitePeer.setFullSyncId(expectedFullSyncId);

            errorReporter.logInfo("Sending full sync to " + satelliteNode + ".");

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
                .fullSync(
                    nodes, storPools, rscs, snapshots, externalFiles, remotes, expectedFullSyncId, FULL_SYNC_RPC_ID
                )
                .build();

            if (waitForAnswer)
            {
                StringBuilder details = new StringBuilder();
                ExtToolsManager extToolsManager = satellitePeer.getExtToolsManager();
                Map<DeviceLayerKind, List<String>> unsupportedLayersWithResons =
                    extToolsManager.getUnsupportedLayersWithReasons();
                Map<DeviceProviderKind, List<String>> unsupportedProvidersWithResons =
                    extToolsManager.getUnsupportedProvidersWithReasons();

                details.append("Supported storage providers: ")
                    .append(extToolsManager.getSupportedProviders().toString().toLowerCase())
                    .append("\nSupported resource layers  : ")
                    .append(extToolsManager.getSupportedLayers().toString().toLowerCase());

                renderUnsupportedDetails(details, unsupportedProvidersWithResons, "storage providers");
                renderUnsupportedDetails(details, unsupportedLayersWithResons, "resource layers");

                flux = ((TcpConnectorPeer) satellitePeer).apiCall(
                        InternalApiConsts.API_FULL_SYNC_DATA,
                        data,
                        true,
                        false
                    )
                    .concatMap(inputStream -> handleFullSyncResponse(satellitePeer, inputStream))
                    .thenMany(
                        Flux.just(
                            ApiCallRcImpl.singletonApiCallRc(
                                ApiCallRcImpl.simpleEntry(
                                    ApiConsts.ConnectionStatus.AUTHENTICATED.getValue(),
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
