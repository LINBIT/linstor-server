package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.NodeRepository;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinitionRepository;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.proto.StorPoolFreeSpaceOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntFreeSpaceOuterClass.MsgIntFreeSpace;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import reactor.core.publisher.Flux;
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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlStorPoolListApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeRepository nodeRepository;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final CtrlClientSerializer clientComSerializer;
    private final Provider<AccessContext> peerAccCtx;
    private final Provider<Long> apiCallId;

    @Inject
    public CtrlStorPoolListApiCallHandler(
        ScopeRunner scopeRunnerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeRepository nodeRepositoryRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        CtrlClientSerializer clientComSerializerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef
    )
    {
        scopeRunner = scopeRunnerRef;
        nodesMapLock = nodesMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeRepository = nodeRepositoryRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        clientComSerializer = clientComSerializerRef;
        peerAccCtx = peerAccCtxRef;
        apiCallId = apiCallIdRef;
    }

    public Flux<byte[]> listStorPools(List<String> nodeNames, List<String> storPoolNames)
    {
        final List<String> upperFilterStorPools =
            storPoolNames.stream().map(String::toUpperCase).collect(toList());
        final List<String> upperFilterNodes =
            nodeNames.stream().map(String::toUpperCase).collect(toList());

        return scopeRunner
            .fluxInTransactionlessScope(
                LockGuard.createDeferred(nodesMapLock.readLock()),
                () -> assembleRequests(upperFilterNodes)
            )
            .collect(Collectors.toList())
            .flatMapMany(freeSpaceAnswers ->
                scopeRunner.callInTransactionlessScope(
                    LockGuard.createDeferred(storPoolDfnMapLock.readLock()),
                    () -> assembleList(upperFilterNodes, upperFilterStorPools, parseFreeSpaces(freeSpaceAnswers))
                )
            );
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> assembleRequests(List<String> upperFilterNodes)
        throws AccessDeniedException
    {
        Stream<Node> nodeStream = upperFilterNodes.isEmpty() ?
            nodeRepository.getMapForView(peerAccCtx.get()).values().stream() :
            upperFilterNodes.stream().map(nodeNameStr -> ctrlApiDataLoader.loadNode(nodeNameStr, true));

        List<Tuple2<NodeName, Flux<ByteArrayInputStream>>> nameAndRequests = nodeStream
            .map(node -> Tuples.of(node.getName(), prepareFreeSpaceApiCall(node)))
            .collect(Collectors.toList());

        return Flux
            .fromIterable(nameAndRequests)
            .flatMap(nameAndRequest -> nameAndRequest.getT2()
                .map(byteStream -> Tuples.of(nameAndRequest.getT1(), byteStream)));
    }

    private byte[] assembleList(
        List<String> upperFilterNodes,
        List<String> upperFilterStorPools,
        Map<StorPool.Key, SpaceInfo> freeSpaceMap
    )
    {
        ArrayList<StorPool.StorPoolApi> storPools = new ArrayList<>();
        try
        {
            storPoolDefinitionRepository.getMapForView(peerAccCtx.get()).values().stream()
                .filter(storPoolDfn ->
                    (
                        upperFilterStorPools.isEmpty() ||
                        upperFilterStorPools.contains(storPoolDfn.getName().value)
                    ) &&
                    !LinStor.DISKLESS_STOR_POOL_NAME.equalsIgnoreCase(storPoolDfn.getName().value)
                )
                .forEach(storPoolDfn ->
                    {
                        try
                        {
                            for (StorPool storPool : storPoolDfn.streamStorPools(peerAccCtx.get())
                                .filter(storPool -> upperFilterNodes.isEmpty() ||
                                    upperFilterNodes.contains(storPool.getNode().getName().value))
                                .collect(toList()))
                            {
                                // fullSyncId and updateId null, as they are not going to be serialized anyway
                                SpaceInfo spaceInfo = freeSpaceMap.get(new StorPool.Key(storPool));
                                Long freeCapacity = null;
                                Long totalCapacity = null;
                                if (spaceInfo != null)
                                {
                                    freeCapacity = spaceInfo.freeCapacity;
                                    totalCapacity = spaceInfo.totalCapacity;
                                }
                                storPools.add(storPool.getApiData(
                                    totalCapacity,
                                    freeCapacity,
                                    peerAccCtx.get(),
                                    null,
                                    null
                                ));
                            }
                        }
                        catch (AccessDeniedException accDeniedExc)
                        {
                            // don't add storpooldfn without access
                        }
                    }
                );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "view storage pool definitions",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }

        return clientComSerializer
            .answerBuilder(ApiConsts.API_LST_STOR_POOL, apiCallId.get())
            .storPoolList(storPools)
            .build();
    }

    private Flux<ByteArrayInputStream> prepareFreeSpaceApiCall(Node node)
    {
        Peer peer = getPeer(node);
        Flux<ByteArrayInputStream> result = Flux.empty();
        if (peer != null)
        {
            result = peer.apiCall(InternalApiConsts.API_REQUEST_FREE_SPACE, new byte[]{})
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

    private Map<StorPool.Key, SpaceInfo> parseFreeSpaces(List<Tuple2<NodeName, ByteArrayInputStream>> freeSpaceAnswers)
        throws IOException, InvalidNameException
    {
        Map<StorPool.Key, SpaceInfo> thinFreeSpaceMap = new HashMap<>();
        for (Tuple2<NodeName, ByteArrayInputStream> freeSpaceAnswer : freeSpaceAnswers)
        {
            NodeName nodeName = freeSpaceAnswer.getT1();
            ByteArrayInputStream freeSpaceMsgDataIn = freeSpaceAnswer.getT2();

            MsgIntFreeSpace freeSpaces = MsgIntFreeSpace.parseDelimitedFrom(freeSpaceMsgDataIn);
            for (StorPoolFreeSpaceOuterClass.StorPoolFreeSpace freeSpace : freeSpaces.getFreeSpaceList())
            {
                thinFreeSpaceMap.put(
                    new StorPool.Key(nodeName, new StorPoolName(freeSpace.getStorPoolName())),
                    new SpaceInfo(freeSpace.getTotalCapacity(), freeSpace.getFreeCapacity())
                );
            }
        }
        return thinFreeSpaceMap;
    }
}
