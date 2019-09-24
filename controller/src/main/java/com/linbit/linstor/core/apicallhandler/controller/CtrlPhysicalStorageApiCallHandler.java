package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.javainternal.s2c.MsgPhysicalDevicesOuterClass.MsgPhysicalDevices;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.LsBlkEntry;
import com.linbit.locks.LockGuardFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Singleton
public class CtrlPhysicalStorageApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlStltSerializer ctrlStltSerializer;

    private final NodeRepository nodeRepository;

    @Inject
    public CtrlPhysicalStorageApiCallHandler(
        ErrorReporter errorReporterRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        NodeRepository nodeRepositoryRef
    )
    {
        this.errorReporter = errorReporterRef;
        this.peerAccCtx = peerAccCtxRef;
        this.scopeRunner = scopeRunnerRef;
        this.lockGuardFactory = lockGuardFactoryRef;
        this.ctrlStltSerializer = ctrlStltSerializerRef;
        this.nodeRepository = nodeRepositoryRef;
    }

    public Flux<Map<NodeName, List<LsBlkEntry>>> listPhysicalStorage()
    {
        return scopeRunner.fluxInTransactionlessScope(
            "list physical storage",
            lockGuardFactory.buildDeferred(LockGuardFactory.LockType.READ, LockGuardFactory.LockObj.NODES_MAP),
            this::listPhysicalStorageInScope
        );
    }

    private Flux<Map<NodeName, List<LsBlkEntry>>> listPhysicalStorageInScope()
    {
        Flux<Map<NodeName, List<LsBlkEntry>>> flux = Flux.empty();
        try
        {
            List<Tuple2<NodeName, Flux<ByteArrayInputStream>>> requests = new ArrayList<>();
            for (Node node : new ArrayList<>(nodeRepository.getMapForView(peerAccCtx.get()).values()))
            {
                requests.add(Tuples.of(node.getName(), node.getPeer(peerAccCtx.get())
                    .apiCall(
                        InternalApiConsts.API_LIST_PHYSICAL_DEVICES,
                        ctrlStltSerializer.headerlessBuilder().requestPhysicalDevices(true).build())
                    .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty())));
            }

            flux = Flux
                .fromIterable(requests)
                .flatMap(nameAndRequest -> nameAndRequest.getT2()
                    .map(byteStream -> Tuples.of(nameAndRequest.getT1(), byteStream)))

            .collectList()
                .flatMapMany(answers ->
                {
                    Map<NodeName, List<LsBlkEntry>> answerMap = new HashMap<>();
                    for (Tuple2<NodeName, ByteArrayInputStream> tup : answers)
                    {
                        answerMap.put(tup.getT1(), parsePhysicalDevices(tup.getT2()));
                    }
                    return Flux.just(answerMap);
                }
            );
        }
        catch (AccessDeniedException accExc)
        {
            errorReporter.reportError(accExc);
        }
        return flux;
    }

    private static List<LsBlkEntry> parsePhysicalDevices(ByteArrayInputStream inputStream)
    {
        try
        {
            MsgPhysicalDevices msgPhysicalDevices =
                MsgPhysicalDevices.parseDelimitedFrom(inputStream);
            return msgPhysicalDevices.getDevicesList().stream()
                .map(lsBlkEntry ->
                    new LsBlkEntry(
                        lsBlkEntry.getName(),
                        lsBlkEntry.getSize(),
                        lsBlkEntry.getRotational(),
                        lsBlkEntry.getParentName(),
                        lsBlkEntry.getKernelName(),
                        lsBlkEntry.getFsType(),
                        lsBlkEntry.getMajor(),
                        lsBlkEntry.getMinor()
                    )
                ).collect(Collectors.toList());
        }
        catch (IOException ioExc)
        {
            throw new LinStorRuntimeException("IOError parsing lsblk answer.", ioExc);
        }
    }

    public static List<JsonGenTypes.PhysicalStorage> groupPhysicalStorageByDevice(
        Map<NodeName, List<LsBlkEntry>> mapData
    )
    {
        Map<JsonGenTypes.PhysicalStorage, JsonGenTypes.PhysicalStorage> phys =
            new TreeMap<>(
                Comparator.comparingLong((JsonGenTypes.PhysicalStorage physicalStorage) -> physicalStorage.size)
                    .thenComparing(physicalStorage -> physicalStorage.rotational));

        for (Map.Entry<NodeName, List<LsBlkEntry>> entry : mapData.entrySet())
        {
            for (LsBlkEntry lsblkEntry : entry.getValue())
            {
                JsonGenTypes.PhysicalStorage physEntry = new JsonGenTypes.PhysicalStorage();
                physEntry.size = lsblkEntry.getSize();
                physEntry.rotational = lsblkEntry.isRotational();

                JsonGenTypes.PhysicalStorageDevice physDev = new JsonGenTypes.PhysicalStorageDevice();
                physDev.device = lsblkEntry.getName();

                if (phys.containsKey(physEntry))
                {
                    phys.get(physEntry).nodes.put(entry.getKey().displayValue, physDev);
                }
                else
                {
                    physEntry.nodes = new HashMap<>();
                    physEntry.nodes.put(entry.getKey().displayValue, physDev);
                    phys.put(physEntry, physEntry);
                }
            }
        }
        return new ArrayList<>(phys.values());
    }
}
