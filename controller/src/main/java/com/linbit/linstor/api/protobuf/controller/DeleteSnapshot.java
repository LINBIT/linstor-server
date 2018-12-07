package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.proto.MsgDelSnapshotOuterClass.MsgDelSnapshot;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_DEL_SNAPSHOT,
    description = "Deletes a snapshot"
)
@Singleton
public class DeleteSnapshot implements ApiCallReactive
{
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapshotDeleteApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public DeleteSnapshot(
        CtrlSnapshotDeleteApiCallHandler ctrlSnapshotDeleteApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlSnapshotDeleteApiCallHandler = ctrlSnapshotDeleteApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgDelSnapshot msgDeleteSnapshot = MsgDelSnapshot.parseDelimitedFrom(msgDataIn);
        return ctrlSnapshotDeleteApiCallHandler
            .deleteSnapshot(
                msgDeleteSnapshot.getRscName(),
                msgDeleteSnapshot.getSnapshotName()
            )
            .transform(responseSerializer::transform);
    }
}
