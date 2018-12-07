package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotRollbackApiCallHandler;
import com.linbit.linstor.proto.MsgRollbackSnapshotOuterClass.MsgRollbackSnapshot;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_ROLLBACK_SNAPSHOT,
    description = "Rolls a resource back to a snapshot state"
)
@Singleton
public class RollbackSnapshot implements ApiCallReactive
{
    private final CtrlSnapshotRollbackApiCallHandler ctrlSnapshotRollbackApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public RollbackSnapshot(
        CtrlSnapshotRollbackApiCallHandler ctrlSnapshotRollbackApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlSnapshotRollbackApiCallHandler = ctrlSnapshotRollbackApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgRollbackSnapshot msgRollbackSnapshot = MsgRollbackSnapshot.parseDelimitedFrom(msgDataIn);

        return ctrlSnapshotRollbackApiCallHandler
            .rollbackSnapshot(
                msgRollbackSnapshot.getRscName(),
                msgRollbackSnapshot.getSnapshotName()
            )
            .transform(responseSerializer::transform);
    }
}
