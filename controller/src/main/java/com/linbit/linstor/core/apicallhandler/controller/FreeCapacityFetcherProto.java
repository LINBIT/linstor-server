package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.NodeRepository;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass.MsgApiCallResponse;
import com.linbit.linstor.proto.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.MsgIntFreeSpaceOuterClass.MsgIntFreeSpace;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Singleton
public class FreeCapacityFetcherProto implements FreeCapacityFetcher
{
    private final ScopeRunner scopeRunner;
    private final ReadWriteLock nodesMapLock;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeRepository nodeRepository;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public FreeCapacityFetcherProto(
        ScopeRunner scopeRunnerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeRepository nodeRepositoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        scopeRunner = scopeRunnerRef;
        nodesMapLock = nodesMapLockRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeRepository = nodeRepositoryRef;
        peerAccCtx = peerAccCtxRef;
    }

    @Override
    public Mono<Map<StorPool.Key, Long>> fetchThinFreeCapacities(Set<NodeName> nodesFilter)
    {
        return fetchThinFreeSpaceInfo(nodesFilter).map(
            freeSpaceInfo -> freeSpaceInfo.getT1().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().freeCapacity
            ))
        );
    }

    @Override
    public Mono<Tuple2<Map<StorPool.Key, SpaceInfo>, List<ApiCallRc>>> fetchThinFreeSpaceInfo(Set<NodeName> nodesFilter)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                LockGuard.createDeferred(nodesMapLock.readLock()),
                () -> assembleRequests(nodesFilter)
            )
            .collect(Collectors.toList())
            .map(this::parseFreeSpaces);
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> assembleRequests(Set<NodeName> nodesFilter)
        throws AccessDeniedException
    {
        Stream<Node> nodeStream = nodesFilter.isEmpty() ?
            nodeRepository.getMapForView(peerAccCtx.get()).values().stream() :
            nodesFilter.stream().map(nodeName -> ctrlApiDataLoader.loadNode(nodeName, true));

        List<Tuple2<NodeName, Flux<ByteArrayInputStream>>> nameAndRequests = nodeStream
            .map(node -> Tuples.of(node.getName(), prepareFreeSpaceApiCall(node)))
            .collect(Collectors.toList());

        return Flux
            .fromIterable(nameAndRequests)
            .flatMap(nameAndRequest -> nameAndRequest.getT2()
                .map(byteStream -> Tuples.of(nameAndRequest.getT1(), byteStream))
            );
    }

    private Flux<ByteArrayInputStream> prepareFreeSpaceApiCall(Node node)
    {
        Peer peer = getPeer(node);
        Flux<ByteArrayInputStream> result = Flux.empty();
        if (peer != null)
        {
            result = peer.apiCall(InternalApiConsts.API_REQUEST_THIN_FREE_SPACE, new byte[]{})
                // No data from disconnected satellites
                .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty());
        }
        return result;
    }

    private Peer getPeer(Node node)
    {
        Peer peer;
        try
        {
            peer = node.getPeer(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access peer for node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return peer;
    }

    private Tuple2<Map<StorPool.Key, SpaceInfo>, List<ApiCallRc>> parseFreeSpaces(
        List<Tuple2<NodeName, ByteArrayInputStream>> freeSpaceAnswers)
    {
        Map<StorPool.Key, SpaceInfo> thinFreeSpaceMap = new HashMap<>();
        List<ApiCallRc> apiCallRcs = new ArrayList<>();

        try
        {

            for (Tuple2<NodeName, ByteArrayInputStream> freeSpaceAnswer : freeSpaceAnswers)
            {
                NodeName nodeName = freeSpaceAnswer.getT1();
                ByteArrayInputStream freeSpaceMsgDataIn = freeSpaceAnswer.getT2();

                MsgIntFreeSpace freeSpaces = MsgIntFreeSpace.parseDelimitedFrom(freeSpaceMsgDataIn);
                for (StorPoolFreeSpace freeSpace : freeSpaces.getFreeSpacesList())
                {
                    if (freeSpace.getErrorsCount() > 0)
                    {
                        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                        for (MsgApiCallResponse msgApiCallResponse : freeSpace.getErrorsList())
                        {
                            apiCallRc.addEntry(ProtoDeserializationUtils.parseApiCallRc(
                                msgApiCallResponse,
                                "Node: '" + nodeName + "', storage pool: '" + freeSpace.getStorPoolName() + "' - "
                            ));
                        }
                        apiCallRcs.add(apiCallRc);
                    }
                    else
                    {
                        thinFreeSpaceMap.put(
                            new StorPool.Key(nodeName, new StorPoolName(freeSpace.getStorPoolName())),
                            new SpaceInfo(freeSpace.getTotalCapacity(), freeSpace.getFreeCapacity())
                        );
                    }
                }
            }
        }
        catch (IOException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }

        return Tuples.of(thinFreeSpaceMap, apiCallRcs);
    }
}
