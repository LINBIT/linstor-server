package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.common.ApiCallResponseOuterClass.ApiCallResponse;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntVlmAllocatedOuterClass.MsgIntVlmAllocated;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntVlmAllocatedOuterClass.VlmAllocated;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Singleton
public class VlmAllocatedFetcherProto implements VlmAllocatedFetcher
{
    private final ScopeRunner scopeRunner;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeRepository nodeRepository;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public VlmAllocatedFetcherProto(
        ScopeRunner scopeRunnerRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeRepository nodeRepositoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeRepository = nodeRepositoryRef;
        peerAccCtx = peerAccCtxRef;
    }

    @Override
    public Mono<Map<Volume.Key, VlmAllocatedResult>> fetchVlmAllocated(
        Set<NodeName> nodesFilter,
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    )
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Fetch volume allocated",
                LockGuard.createDeferred(
                    nodesMapLock.readLock(), rscDfnMapLock.readLock(), storPoolDfnMapLock.readLock()),
                () -> requestVlmAllocated(nodesFilter, storPoolFilter, resourceFilter),
                MDC.getCopyOfContextMap()
            )
            .collect(Collectors.toList())
            .map(this::parseVlmAllocated);
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> requestVlmAllocated(
        Set<NodeName> nodesFilter,
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    )
        throws AccessDeniedException
    {
        Stream<Node> nodeStream = nodesFilter.isEmpty() ?
            nodeRepository.getMapForView(peerAccCtx.get()).values().stream() :
            nodesFilter.stream().map(nodeName -> ctrlApiDataLoader.loadNode(nodeName, true));

        Stream<Node> nodeWithThinStream = nodeStream.filter(node -> hasThinVlms(node, storPoolFilter, resourceFilter));

        List<Tuple2<NodeName, Flux<ByteArrayInputStream>>> nameAndRequests = nodeWithThinStream
            .map(node -> Tuples.of(node.getName(), requestVlmAllocatedOnNode(node, storPoolFilter, resourceFilter)))
            .collect(Collectors.toList());

        return Flux
            .fromIterable(nameAndRequests)
            .flatMap(nameAndRequest -> nameAndRequest.getT2()
                .map(byteStream -> Tuples.of(nameAndRequest.getT1(), byteStream))
            );
    }

    private boolean hasThinVlms(
        Node node,
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    )
    {
        return streamStorPools(node)
            .filter(storPool -> storPool.getDeviceProviderKind().usesThinProvisioning())
            .filter(storPool -> storPoolFilter.isEmpty() || storPoolFilter.contains(storPool.getName()))
            .flatMap(this::streamVolumes)
            .map(vlmData -> vlmData.getVolume().getResourceDefinition())
            .map(ResourceDefinition::getName)
            .anyMatch(rscName -> resourceFilter.isEmpty() || resourceFilter.contains(rscName));
    }

    private Flux<ByteArrayInputStream> requestVlmAllocatedOnNode(
        Node node,
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    )
    {
        Peer peer = getPeer(node);
        Flux<ByteArrayInputStream> result = Flux.empty();
        if (peer != null)
        {
            result = peer
                .apiCall(
                    InternalApiConsts.API_REQUEST_VLM_ALLOCATED,
                    ctrlStltSerializer.headerlessBuilder().filter(
                        Collections.emptySet(),
                        storPoolFilter,
                        resourceFilter
                    ).build()
                )
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

    private Stream<VlmProviderObject<Resource>> streamVolumes(StorPool storPool)
    {
        Stream<VlmProviderObject<Resource>> vlmStream;
        try
        {
            vlmStream = storPool.getVolumes(peerAccCtx.get()).stream();
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accessDeniedExc,
                "stream volumes of " + storPool,
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        return vlmStream;
    }

    private @Nullable Peer getPeer(Node node)
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

    private Map<Volume.Key, VlmAllocatedResult> parseVlmAllocated(
        List<Tuple2<NodeName, ByteArrayInputStream>> vlmAllocatedAnswers)
    {
        Map<Volume.Key, VlmAllocatedResult> vlmAllocatedCapacities = new HashMap<>();

        try
        {
            for (Tuple2<NodeName, ByteArrayInputStream> vlmAllocatedAnswer : vlmAllocatedAnswers)
            {
                NodeName nodeName = vlmAllocatedAnswer.getT1();
                ByteArrayInputStream vlmAllocatedMsgDataIn = vlmAllocatedAnswer.getT2();

                MsgIntVlmAllocated nodeVlmAllocated = MsgIntVlmAllocated.parseDelimitedFrom(vlmAllocatedMsgDataIn);
                for (VlmAllocated vlmAllocated : nodeVlmAllocated.getAllocatedCapacitiesList())
                {
                    ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                    for (ApiCallResponse msgApiCallResponse : vlmAllocated.getErrorsList())
                    {
                        apiCallRc.addEntry(ProtoDeserializationUtils.parseApiCallRc(
                            msgApiCallResponse,
                            "Node: '" + nodeName +
                                "', resource: '" + vlmAllocated.getRscName() +
                                "', volume: " + vlmAllocated.getVlmNr() + " - "
                        ));
                    }

                    vlmAllocatedCapacities.put(
                        new Volume.Key(
                            nodeName,
                            new ResourceName(vlmAllocated.getRscName()),
                            new VolumeNumber(vlmAllocated.getVlmNr())
                        ),
                        new VlmAllocatedResult(vlmAllocated.getAllocated(), apiCallRc)
                    );
                }
            }
        }
        catch (IOException | InvalidNameException | ValueOutOfRangeException exc)
        {
            throw new ImplementationError(exc);
        }

        return vlmAllocatedCapacities;
    }
}
