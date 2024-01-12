package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.SnapshotInternalCallHandler;
import com.linbit.linstor.proto.javainternal.s2c.MsgSnapshotRollbackResultOuterClass.MsgSnapshotRollbackResult;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_SNAPSHOT_ROLLBACK_RESULT,
    description = "Called by the satellite to notify the controller about success or failure of a rollback",
    transactional = true
)
@Singleton
public class NotifySnapshotRollbackResult implements ApiCallReactive
{
    private final SnapshotInternalCallHandler snapInternalCallHandler;

    @Inject
    public NotifySnapshotRollbackResult(
        SnapshotInternalCallHandler snapInternalCallHandlerRef
    )
    {
        snapInternalCallHandler = snapInternalCallHandlerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn) throws IOException
    {
        MsgSnapshotRollbackResult msgSnapRollbackResult = MsgSnapshotRollbackResult.parseDelimitedFrom(msgDataIn);

        snapInternalCallHandler.handleSnapshotRollbackResult(
            msgSnapRollbackResult.getRsc().getNodeName(),
            msgSnapRollbackResult.getRsc().getName(),
            msgSnapRollbackResult.getSuccess()
        );
        /*
         * Although we do not return a meaningful Flux, we still need to be a reactive ApiCall, since non-reactive
         * ApiCalls are automatically scoped with runs into a conflict of the final "recoverIfNeeded" call from the
         * above handleSnapshotRollbackResult method
         */
        return Flux.empty();
    }
}
