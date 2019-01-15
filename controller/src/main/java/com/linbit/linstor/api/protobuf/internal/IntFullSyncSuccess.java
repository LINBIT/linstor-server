package com.linbit.linstor.api.protobuf.internal;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.pojo.CapacityInfoPojo;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlFullSyncApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.internal.StorPoolInternalCallHandler;
import com.linbit.linstor.proto.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.MsgIntFullSyncSuccessOuterClass.MsgIntFullSyncSuccess;
import com.linbit.locks.LockGuard;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

@ProtobufApiCall(
    name = InternalApiConsts.API_FULL_SYNC_SUCCESS,
    description = "Satellite failed to apply our full sync"
)
@Singleton
public class IntFullSyncSuccess implements ApiCallReactive
{
    private final ScopeRunner scopeRunner;
    private final StorPoolInternalCallHandler storPoolApiCallHandler;
    private final CtrlFullSyncApiCallHandler ctrlFullSyncApiCallHandler;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock storPoolDfnMapLock;

    @Inject
    public IntFullSyncSuccess(
        ScopeRunner scopeRunnerRef,
        StorPoolInternalCallHandler storPoolApiCallHandlerRef,
        CtrlFullSyncApiCallHandler ctrlFullSyncApiCallHandlerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef
    )
    {
        scopeRunner = scopeRunnerRef;
        storPoolApiCallHandler = storPoolApiCallHandlerRef;
        ctrlFullSyncApiCallHandler = ctrlFullSyncApiCallHandlerRef;
        nodesMapLock = nodesMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgIntFullSyncSuccess msgIntFullSyncSuccess = MsgIntFullSyncSuccess.parseDelimitedFrom(msgDataIn);

        List<CapacityInfoPojo> capacityInfoPojoList = new ArrayList<>();
        for (StorPoolFreeSpace protoFreeSpace : msgIntFullSyncSuccess.getFreeSpaceList())
        {
            capacityInfoPojoList.add(
                new CapacityInfoPojo(
                    UUID.fromString(protoFreeSpace.getStorPoolUuid()),
                    protoFreeSpace.getStorPoolName(),
                    protoFreeSpace.getFreeCapacity(),
                    protoFreeSpace.getTotalCapacity()
                )
            );
        }

        return scopeRunner
            .fluxInTransactionalScope(
                "Handle full sync success",
                LockGuard.createDeferred(nodesMapLock.writeLock(), storPoolDfnMapLock.writeLock()),
                () ->
                {
                    storPoolApiCallHandler.updateRealFreeSpace(capacityInfoPojoList);
                    return Flux.empty();
                }
            )
            .thenMany(ctrlFullSyncApiCallHandler.fullSyncSuccess().thenMany(Flux.<byte[]>empty()));
    }

}
