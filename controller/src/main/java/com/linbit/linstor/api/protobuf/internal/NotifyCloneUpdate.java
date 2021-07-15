package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.RscDfnInternalCallHandler;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntCloneUpdateOuterClass.MsgIntCloneUpdate;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_CLONE_UPDATE,
    description = "Called by the satellite to notify the controller that a clone background task finished/aborted",
    transactional = true
)
@Singleton
public class NotifyCloneUpdate implements ApiCallReactive
{
    private final RscDfnInternalCallHandler rscDfnInternalCallHandler;

    @Inject
    public NotifyCloneUpdate(RscDfnInternalCallHandler rscDfnInternalCallHandlerRef)
    {
        rscDfnInternalCallHandler = rscDfnInternalCallHandlerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataInRef) throws IOException
    {
        MsgIntCloneUpdate cloneUpdate = MsgIntCloneUpdate.parseDelimitedFrom(msgDataInRef);
        return rscDfnInternalCallHandler.handleCloneUpdate(
            cloneUpdate.getRscName(),
            cloneUpdate.getVlmNr(),
            cloneUpdate.getSuccess()
        ).thenMany(Flux.empty());
    }
}
