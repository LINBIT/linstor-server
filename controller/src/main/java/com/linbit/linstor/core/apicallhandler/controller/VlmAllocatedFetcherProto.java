package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
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
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.locks.LockGuard;
import com.linbit.utils.RegexMatcher;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.regex.Pattern;
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

    @Inject
    public VlmAllocatedFetcherProto(
        ScopeRunner scopeRunnerRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeRepository nodeRepositoryRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeRepository = nodeRepositoryRef;
    }

    @Override
    public Mono<Map<Volume.Key, VlmAllocatedResult>> fetchVlmAllocated(
        Set<NodeName> nodesFilter,
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    )
    {
        return fetchVlmAllocated(() -> requestVlmAllocated(nodesFilter, storPoolFilter, resourceFilter));
    }

    @Override
    public Mono<Map<Volume.Key, VlmAllocatedResult>> fetchVlmAllocated(
        List<Pattern> nodeNameFilters,
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    )
    {
        return fetchVlmAllocated(() -> requestVlmAllocated(nodeNameFilters, storPoolFilter, resourceFilter));
    }

    private Mono<Map<Volume.Key, VlmAllocatedResult>> fetchVlmAllocated(
        Callable<Flux<Tuple2<NodeName, ByteArrayInputStream>>> requestsSupplier
    )
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Fetch volume allocated",
                LockGuard.createDeferred(
                    nodesMapLock.readLock(), rscDfnMapLock.readLock(), storPoolDfnMapLock.readLock()),
                requestsSupplier,
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
    {
        Stream<Node> nodeStream = nodesFilter.isEmpty() ?
            nodeRepository.getMapForView().values().stream() :
            nodesFilter.stream().map(nodeName -> ctrlApiDataLoader.loadNode(nodeName));

        return buildVlmAllocatedRequests(nodeStream, storPoolFilter, resourceFilter);
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> requestVlmAllocated(
        List<Pattern> nodeNameFilters,
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    )
    {
        Stream<Node> nodeStream = nodeRepository.getMapForView().values().stream()
            .filter(node -> RegexMatcher.matchesAny(nodeNameFilters, node.getName().displayValue));

        return buildVlmAllocatedRequests(nodeStream, storPoolFilter, resourceFilter);
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> buildVlmAllocatedRequests(
        Stream<Node> nodeStream,
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    )
    {
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
        return getPeer(node).apiCall(
            InternalApiConsts.API_REQUEST_VLM_ALLOCATED,
            ctrlStltSerializer.headerlessBuilder()
                .filter(
                    Collections.emptySet(),
                    storPoolFilter,
                    resourceFilter
                )
                .build()
        )
            // No data from disconnected satellites
            .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty());
    }

    private Stream<StorPool> streamStorPools(Node node)
    {
        Stream<StorPool> storPoolStream;
        storPoolStream = node.streamStorPools();
        return storPoolStream;
    }

    private Stream<VlmProviderObject<Resource>> streamVolumes(StorPool storPool)
    {
        Stream<VlmProviderObject<Resource>> vlmStream;
        vlmStream = storPool.getVolumes().stream();
        return vlmStream;
    }

    private Peer getPeer(Node node)
    {
        Peer peer;
        peer = node.getPeer();
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
