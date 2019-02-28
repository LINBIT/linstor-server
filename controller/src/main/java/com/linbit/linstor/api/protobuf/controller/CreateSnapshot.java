package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtApiCallHandler;
import com.linbit.linstor.proto.requests.MsgCrtSnapshotOuterClass.MsgCrtSnapshot;
import com.linbit.linstor.proto.common.SnapshotDfnOuterClass;
import com.linbit.linstor.proto.common.SnapshotDfnOuterClass.SnapshotDfn;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Collectors;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_SNAPSHOT,
    description = "Creates a snapshot"
)
@Singleton
public class CreateSnapshot implements ApiCallReactive
{
    private final CtrlSnapshotCrtApiCallHandler ctrlSnapshotCrtApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public CreateSnapshot(
        CtrlSnapshotCrtApiCallHandler ctrlSnapshotCrtApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlSnapshotCrtApiCallHandler = ctrlSnapshotCrtApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtSnapshot msgCrtSnapshot = MsgCrtSnapshot.parseDelimitedFrom(msgDataIn);
        SnapshotDfn snapshotDfn = msgCrtSnapshot.getSnapshotDfn();

        return ctrlSnapshotCrtApiCallHandler
            .createSnapshot(
                snapshotDfn.getSnapshotsList().stream()
                    .map(SnapshotDfnOuterClass.Snapshot::getNodeName)
                    .collect(Collectors.toList()),
                snapshotDfn.getRscName(),
                snapshotDfn.getSnapshotName()
            )
            .transform(responseSerializer::transform);
    }
}
