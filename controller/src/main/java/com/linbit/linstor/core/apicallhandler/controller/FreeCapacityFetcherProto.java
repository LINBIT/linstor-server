package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.common.ApiCallResponseOuterClass.ApiCallResponse;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntFreeSpaceOuterClass.MsgIntFreeSpace;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.RegexMatcher;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
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
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeRepository nodeRepository;

    @Inject
    public FreeCapacityFetcherProto(
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeRepository nodeRepositoryRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeRepository = nodeRepositoryRef;
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

    @SuppressWarnings("resource")
    @Override
    public Mono<Map<StorPool.Key, Tuple2<SpaceInfo, List<ApiCallRc>>>> fetchThinFreeSpaceInfo(Set<NodeName> nodesFilter)
    {
        return fetchThinFreeSpaceInfo(() -> assembleRequests(nodesFilter));
    }

    @SuppressWarnings("resource")
    @Override
    public Mono<Map<StorPool.Key, Tuple2<SpaceInfo, List<ApiCallRc>>>> fetchThinFreeSpaceInfo(
        List<Pattern> nodeNameFilters
    )
    {
        return fetchThinFreeSpaceInfo(() -> assembleRequests(nodeNameFilters));
    }

    private Mono<Map<StorPool.Key, Tuple2<SpaceInfo, List<ApiCallRc>>>> fetchThinFreeSpaceInfo(
        Callable<Flux<Tuple2<NodeName, ByteArrayInputStream>>> requestsSupplier
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Fetch thin capacity info",
            lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.STOR_POOL_DFN_MAP),
            () -> requestsSupplier.call().flatMap(this::parseFreeSpaces),
            MDC.getCopyOfContextMap()
        )
            .collectMap(
            t -> t.getT1(),
            t -> t.getT2()
        );
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> assembleRequests(Set<NodeName> nodesFilter)
    {
        Stream<Node> nodeStream = nodesFilter.isEmpty() ?
            nodeRepository.getMapForView().values().stream() :
            nodesFilter.stream().map(nodeName -> ctrlApiDataLoader.loadNode(nodeName));

        return buildFreeSpaceRequests(nodeStream);
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> assembleRequests(List<Pattern> nodeNameFilters)
    {
        Stream<Node> nodeStream = nodeRepository.getMapForView().values().stream()
            .filter(node -> RegexMatcher.matchesAny(nodeNameFilters, node.getName().displayValue));

        return buildFreeSpaceRequests(nodeStream);
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> buildFreeSpaceRequests(Stream<Node> nodeStream)
    {
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
        return getPeer(node).apiCall(InternalApiConsts.API_REQUEST_THIN_FREE_SPACE, new byte[0])
            // No data from disconnected satellites
            .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty());
    }

    private Stream<StorPool> streamStorPools(Node node)
    {
        Stream<StorPool> storPoolStream;
        storPoolStream = node.streamStorPools();
        return storPoolStream;
    }

    private Peer getPeer(Node node)
    {
        Peer peer;
        peer = node.getPeer();
        return peer;
    }

    @SuppressWarnings("resource")
    private Flux<Tuple2<StorPool.Key, Tuple2<SpaceInfo, List<ApiCallRc>>>> parseFreeSpaces(
        Tuple2<NodeName, ByteArrayInputStream> freeSpaceAnswer
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Parse thin free space response",
            lockGuardFactory.createDeferred()
                .read(LockObj.NODES_MAP)
                .write(LockObj.STOR_POOL_DFN_MAP)
                .build(),
            () -> parseFreeSpacesInTransaction(freeSpaceAnswer)
        );
    }

    private Flux<Tuple2<StorPool.Key, Tuple2<SpaceInfo, List<ApiCallRc>>>> parseFreeSpacesInTransaction(
        Tuple2<NodeName, ByteArrayInputStream> freeSpaceAnswer
    )
    {
        List<Tuple2<StorPool.Key, Tuple2<SpaceInfo, List<ApiCallRc>>>> ret = new ArrayList<>();
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
                StorPool storPool = nodeRepository.get(nodeName).getStorPool(storPoolName);
                storPool.getFreeSpaceTracker().setCapacityInfo(freeCapacity, totalCapacity);

                ctrlTransactionHelper.commit();
            }
        }
        catch (IOException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        return Flux.just(ret.toArray(new Tuple2[0]));
    }
}
