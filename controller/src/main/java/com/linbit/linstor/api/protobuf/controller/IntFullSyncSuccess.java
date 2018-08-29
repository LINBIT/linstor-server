package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Named;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.pojo.FreeSpacePojo;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlFullSyncApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolApiCallHandler;
import com.linbit.linstor.netcom.Peer;
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
public class IntFullSyncSuccess implements ApiCallReactive
{
    private final ScopeRunner scopeRunner;
    private final CtrlStorPoolApiCallHandler storPoolApiCallHandler;
    private final CtrlFullSyncApiCallHandler ctrlFullSyncApiCallHandler;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final Peer satellite;

    @Inject
    public IntFullSyncSuccess(
        ScopeRunner scopeRunnerRef,
        CtrlStorPoolApiCallHandler storPoolApiCallHandlerRef,
        CtrlFullSyncApiCallHandler ctrlFullSyncApiCallHandlerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        Peer satelliteRef
    )
    {
        scopeRunner = scopeRunnerRef;
        storPoolApiCallHandler = storPoolApiCallHandlerRef;
        ctrlFullSyncApiCallHandler = ctrlFullSyncApiCallHandlerRef;
        nodesMapLock = nodesMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        satellite = satelliteRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgIntFullSyncSuccess msgIntFullSyncSuccess = MsgIntFullSyncSuccess.parseDelimitedFrom(msgDataIn);

        List<FreeSpacePojo> freeSpacePojoList = new ArrayList<>();
        for (StorPoolFreeSpace protoFreeSpace : msgIntFullSyncSuccess.getFreeSpaceList())
        {
            freeSpacePojoList.add(
                new FreeSpacePojo(
                    UUID.fromString(protoFreeSpace.getStorPoolUuid()),
                    protoFreeSpace.getStorPoolName(),
                    protoFreeSpace.getFreeCapacity()
                )
            );
        }

        return scopeRunner
            .fluxInTransactionalScope(
                LockGuard.createDeferred(nodesMapLock.writeLock(), storPoolDfnMapLock.writeLock()),
                () ->
                {
                    storPoolApiCallHandler.updateRealFreeSpace(freeSpacePojoList);
                    return Flux.empty();
                }
            )
            .thenMany(ctrlFullSyncApiCallHandler.fullSyncSuccess(satellite).thenMany(Flux.<byte[]>empty()));
    }

}
