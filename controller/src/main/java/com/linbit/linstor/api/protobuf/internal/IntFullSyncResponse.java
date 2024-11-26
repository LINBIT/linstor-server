package com.linbit.linstor.api.protobuf.internal;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.CapacityInfoPojo;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlFullSyncResponseApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlFullSyncResponseApiCallHandler.FullSyncSuccessContext;
import com.linbit.linstor.core.apicallhandler.controller.internal.StorPoolInternalCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntFullSyncResponseOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntFullSyncResponseOuterClass.MsgIntFullSyncResponse;
import com.linbit.linstor.storage.ProcCryptoEntry;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_FULL_SYNC_RESPONSE,
    description = "Satellite's response to full sync data"
)
@Singleton
public class IntFullSyncResponse implements ApiCallReactive
{
    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final StorPoolInternalCallHandler storPoolApiCallHandler;
    private final CtrlFullSyncResponseApiCallHandler ctrlFullSyncApiCallHandler;
    private final Provider<Peer> satelliteProvider;

    @Inject
    public IntFullSyncResponse(
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        StorPoolInternalCallHandler storPoolApiCallHandlerRef,
        CtrlFullSyncResponseApiCallHandler ctrlFullSyncApiCallHandlerRef,
        Provider<Peer> satelliteProviderRef
    )
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        nodesMapLock = nodesMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        storPoolApiCallHandler = storPoolApiCallHandlerRef;
        ctrlFullSyncApiCallHandler = ctrlFullSyncApiCallHandlerRef;
        satelliteProvider = satelliteProviderRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn) throws IOException
    {
        return processReactive(satelliteProvider.get(), msgDataIn, null);
    }

    public Flux<byte[]> processReactive(Peer satellitePeerRef, InputStream msgDataIn, ResponseContext context)
        throws IOException
    {
        final ResponseContext ctx;
        if (context == null)
        {
            ctx = CtrlNodeApiCallHandler.makeNodeContext(
                ApiOperation.makeCreateOperation(),
                satellitePeerRef.getNode().getName().displayValue
            );
        }
        else
        {
            ctx = context;
        }

        MsgIntFullSyncResponse msgIntFullSyncResponse = MsgIntFullSyncResponse.parseDelimitedFrom(msgDataIn);

        Flux<byte[]> flux;
        if (msgIntFullSyncResponse.getFullSyncResult() == MsgIntFullSyncResponseOuterClass.FullSyncResult.SUCCESS)
        {
            List<CapacityInfoPojo> capacityInfoPojoList = new ArrayList<>();
            for (StorPoolFreeSpace protoFreeSpace : msgIntFullSyncResponse.getFreeSpaceList())
            {
                capacityInfoPojoList.add(
                    new CapacityInfoPojo(
                        UUID.fromString(protoFreeSpace.getStorPoolUuid()),
                        protoFreeSpace.getStorPoolName(),
                        protoFreeSpace.getFreeCapacity(),
                        protoFreeSpace.getTotalCapacity(),
                        ProtoDeserializationUtils.parseApiCallRcList(protoFreeSpace.getErrorsList())
                    )
                );
            }

            List<ProcCryptoEntry> cryptoEntries = msgIntFullSyncResponse.getCryptoEntriesList().stream()
                .map(ce -> new ProcCryptoEntry(
                    ce.getName(),
                    ce.getDriver(),
                    ProcCryptoEntry.CryptoType.fromString(ce.getType()),
                    ce.getPriority()))
                .collect(Collectors.toList());
            errorReporter.logTrace("CryptoEntries for %s: %s",
                satellitePeerRef.getNode().getName(),
                cryptoEntries.stream().map(ProcCryptoEntry::getName).collect(Collectors.toList()));
            satellitePeerRef.getNode().setCryptoEntries(cryptoEntries);

            flux = scopeRunner
                .fluxInTransactionalScope(
                    "Handle full sync api success",
                    LockGuard.createDeferred(nodesMapLock.writeLock(), storPoolDfnMapLock.writeLock()),
                    () -> updateCapacities(satellitePeerRef, capacityInfoPojoList)
                )
                .thenMany(
                    ctrlFullSyncApiCallHandler.fullSyncSuccess(
                        new FullSyncSuccessContext(
                            satellitePeerRef,
                            msgIntFullSyncResponse.getNodePropsToSetMap(),
                            msgIntFullSyncResponse.getNodePropKeysToDeleteList(),
                            msgIntFullSyncResponse.getNodePropNamespacesToDeleteList()
                        ),
                        ctx
                    )
                    .thenMany(Flux.empty())
                );
        }
        else
        {
            ApiConsts.ConnectionStatus connectionStatus;
            switch (msgIntFullSyncResponse.getFullSyncResult())
            {
                case FAIL_MISSING_REQUIRED_EXT_TOOLS:
                    connectionStatus = ApiConsts.ConnectionStatus.MISSING_EXT_TOOLS;
                    break;
                case SUCCESS:
                    throw new ImplementationError(
                        "unexpected enum type: " + msgIntFullSyncResponse.getFullSyncResult()
                    );
                case UNRECOGNIZED:
                case FAIL_UNKNOWN:
                default:
                    connectionStatus = ApiConsts.ConnectionStatus.FULL_SYNC_FAILED;
                    break;
            }
            flux = ctrlFullSyncApiCallHandler.fullSyncFailed(
                satellitePeerRef,
                connectionStatus
            )
                .thenMany(Flux.empty());
        }
        return flux;
    }


    private Flux<?> updateCapacities(Peer satellitePeerRef, List<CapacityInfoPojo> capacityInfoPojoList)
    {
        storPoolApiCallHandler.updateRealFreeSpace(satellitePeerRef, capacityInfoPojoList);
        return Flux.empty();
    }
}
