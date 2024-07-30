package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPool.Key;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.common.ApiCallResponseOuterClass.ApiCallResponse;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntFreeSpaceOuterClass.MsgIntFreeSpace;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Singleton
public class FreeCapacityFetcherProto implements FreeCapacityFetcher
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeRepository nodeRepository;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public FreeCapacityFetcherProto(
        @SystemContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeRepository nodeRepositoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeRepository = nodeRepositoryRef;
        peerAccCtx = peerAccCtxRef;
    }

    @Override
    public Mono<Map<StorPool.Key, Long>> fetchThinFreeCapacities(Set<NodeName> nodesFilter)
    {
        return fetchThinFreeSpaceInfo(nodesFilter).map(
            freeSpaceInfo -> {
                MDC.setContextMap(MDC.getCopyOfContextMap());
                return freeSpaceInfo.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().getT1().freeCapacity
                ));
            }
        );
    }

    @Override
    public Mono<Map<StorPool.Key, Tuple2<SpaceInfo, List<ApiCallRc>>>> fetchThinFreeSpaceInfo(Set<NodeName> nodesFilter)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Fetch thin capacity info",
            lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.STOR_POOL_DFN_MAP),
            () -> assembleRequests(nodesFilter).flatMap(this::parseFreeSpaces),
            MDC.getCopyOfContextMap()
        )
            .collectMap(
            t -> t.getT1(),
            t -> t.getT2()
        );
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> assembleRequests(Set<NodeName> nodesFilter)
        throws AccessDeniedException
    {
        Stream<Node> nodeStream = nodesFilter.isEmpty() ?
            nodeRepository.getMapForView(peerAccCtx.get()).values().stream() :
            nodesFilter.stream().map(nodeName -> ctrlApiDataLoader.loadNode(nodeName, true));

        Stream<Node> nodeWithThinStream = nodeStream.filter(this::hasThinPools);

        List<Tuple2<NodeName, Flux<ByteArrayInputStream>>> nameAndRequests = nodeWithThinStream
            .map(node -> Tuples.of(node.getName(), prepareFreeSpaceApiCall(node)))
            .collect(Collectors.toList());

        return Flux
            .fromIterable(nameAndRequests)
            .flatMap(nameAndRequest -> nameAndRequest.getT2()
                .map(byteStream -> Tuples.of(nameAndRequest.getT1(), byteStream))
            );
    }

    private boolean hasThinPools(Node node)
    {
        return streamStorPools(node)
            .map(StorPool::getDeviceProviderKind)
            .anyMatch(kind -> kind.usesThinProvisioning() && !DeviceProviderKind.DISKLESS.equals(kind));
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

    private Stream<StorPool> streamStorPools(Node node)
    {
        Stream<StorPool> storPoolStream;
        try
        {
            storPoolStream = node.streamStorPools(peerAccCtx.get());
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accessDeniedExc,
                "stream storage pools of " + node,
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return storPoolStream;
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

    private Flux<Tuple2<StorPool.Key, Tuple2<SpaceInfo, List<ApiCallRc>>>> parseFreeSpaces(
        Tuple2<NodeName, ByteArrayInputStream> freeSpaceAnswer
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Parse thin free space response",
            lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.STOR_POOL_DFN_MAP),
            () -> parseFreeSpacesInTransaction(freeSpaceAnswer)
        );
    }

    private Flux<Tuple2<StorPool.Key, Tuple2<SpaceInfo, List<ApiCallRc>>>> parseFreeSpacesInTransaction(
        Tuple2<NodeName, ByteArrayInputStream> freeSpaceAnswer
    )
    {
        List<Tuple2<Key, Tuple2<SpaceInfo, List<ApiCallRc>>>> ret = new ArrayList<>();
        try
        {
            NodeName nodeName = freeSpaceAnswer.getT1();
            ByteArrayInputStream freeSpaceMsgDataIn = freeSpaceAnswer.getT2();

            MsgIntFreeSpace freeSpaces = MsgIntFreeSpace.parseDelimitedFrom(freeSpaceMsgDataIn);
            for (StorPoolFreeSpace freeSpaceInfo : freeSpaces.getFreeSpacesList())
            {
                List<ApiCallRc> apiCallRcs = new ArrayList<>();
                if (freeSpaceInfo.getErrorsCount() > 0)
                {
                    ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                    for (ApiCallResponse msgApiCallResponse : freeSpaceInfo.getErrorsList())
                    {
                        apiCallRc.addEntry(
                            ProtoDeserializationUtils.parseApiCallRc(
                                msgApiCallResponse,
                                "Node: '" + nodeName + "', storage pool: '" + freeSpaceInfo.getStorPoolName() +
                                    "' - "
                            )
                        );
                    }
                    apiCallRcs.add(apiCallRc);
                }

                StorPoolName storPoolName = new StorPoolName(freeSpaceInfo.getStorPoolName());
                long freeCapacity = freeSpaceInfo.getFreeCapacity();
                long totalCapacity = freeSpaceInfo.getTotalCapacity();

                ret.add(
                    Tuples.of(
                        new StorPool.Key(nodeName, storPoolName),
                        Tuples.of(
                            new SpaceInfo(totalCapacity, freeCapacity),
                            apiCallRcs
                        )
                    )
                );

                // also update storage pool's freespacemanager
                StorPool storPool = nodeRepository.get(apiCtx, nodeName).getStorPool(apiCtx, storPoolName);
                storPool.getFreeSpaceTracker().setCapacityInfo(apiCtx, freeCapacity, totalCapacity);

                ctrlTransactionHelper.commit();
            }
        }
        catch (IOException | InvalidNameException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return Flux.just(ret.toArray(new Tuple2[0]));
    }
}
