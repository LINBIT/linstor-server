package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotRestoreApiCallHandler;
import com.linbit.linstor.proto.requests.MsgRestoreSnapshotRscOuterClass;
import com.linbit.linstor.proto.requests.MsgRestoreSnapshotRscOuterClass.MsgRestoreSnapshotRsc;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = ApiConsts.API_RESTORE_SNAPSHOT,
    description = "Restores a snapshot"
)
@Singleton
public class RestoreSnapshot implements ApiCallReactive
{
    private final CtrlSnapshotRestoreApiCallHandler ctrlSnapshotRestoreApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public RestoreSnapshot(
        CtrlSnapshotRestoreApiCallHandler ctrlSnapshotRestoreApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlSnapshotRestoreApiCallHandler = ctrlSnapshotRestoreApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgRestoreSnapshotRsc msgRestoreSnapshotRsc = MsgRestoreSnapshotRsc.parseDelimitedFrom(msgDataIn);

        return ctrlSnapshotRestoreApiCallHandler.restoreSnapshot(
            msgRestoreSnapshotRsc.getNodesList().stream()
                .map(MsgRestoreSnapshotRscOuterClass.RestoreNode::getName)
                .collect(Collectors.toList()),
            msgRestoreSnapshotRsc.getFromResourceName(),
            msgRestoreSnapshotRsc.getFromSnapshotName(),
            msgRestoreSnapshotRsc.getToResourceName()
        ).transform(responseSerializer::transform);
    }
}
