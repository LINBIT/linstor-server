package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.pojo.CapacityInfoPojo;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlFullSyncResponseApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.internal.StorPoolInternalCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntFullSyncResponseOuterClass.MsgIntFullSyncResponse;
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

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_FULL_SYNC_RESPONSE,
    description = "Satellite's response to full sync data",
    transactional = true
)
@Singleton
public class IntFullSyncResponse implements ApiCallReactive
{
    private final ScopeRunner scopeRunner;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final StorPoolInternalCallHandler storPoolApiCallHandler;
    private final CtrlFullSyncResponseApiCallHandler ctrlFullSyncApiCallHandler;
    private final Provider<Peer> satelliteProvider;

    @Inject
    public IntFullSyncResponse(
        ScopeRunner scopeRunnerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        StorPoolInternalCallHandler storPoolApiCallHandlerRef,
        CtrlFullSyncResponseApiCallHandler ctrlFullSyncApiCallHandlerRef,
        Provider<Peer> satelliteProviderRef
    )
    {
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
        if (msgIntFullSyncResponse.getSuccess())
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

            flux = scopeRunner
                .fluxInTransactionalScope(
                    "Handle full sync api success",
                    LockGuard.createDeferred(nodesMapLock.writeLock(), storPoolDfnMapLock.writeLock()),
                    () -> updateCapacities(satellitePeerRef, capacityInfoPojoList)
                )
                .thenMany(ctrlFullSyncApiCallHandler.fullSyncSuccess(satellitePeerRef, ctx)
                    .thenMany(Flux.<byte[]>empty())
                );
        }
        else
        {
            flux = ctrlFullSyncApiCallHandler.fullSyncFailed(satellitePeerRef)
                .thenMany(Flux.<byte[]>empty());
        }
        return flux;
    }


    private Flux<?> updateCapacities(Peer satellitePeerRef, List<CapacityInfoPojo> capacityInfoPojoList)
    {
        storPoolApiCallHandler.updateRealFreeSpace(satellitePeerRef, capacityInfoPojoList);
        return Flux.empty();
    }
}
