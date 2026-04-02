package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.VlmDfnInternalCallHandler;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntDrbdSetVlmUpToDateOuterClass.MsgIntDrbdSetVlmUpToDate;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

import reactor.core.publisher.Flux;

/**
 * Handles satellite requests to set a volume as UpToDate.
 */
@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_DRBD_SET_VLM_UP_TO_DATE,
    description = "Satellite notifies controller that it set a DRBD volume to UpToDate for a given resource"
)
@Singleton
public class HandleNotifySetUpToDateVlm implements ApiCallReactive
{
    private final VlmDfnInternalCallHandler vlmDfnInternalCallHandler;

    @Inject
    public HandleNotifySetUpToDateVlm(VlmDfnInternalCallHandler vlmDfnInternalCallHandlerRef)
    {
        vlmDfnInternalCallHandler = vlmDfnInternalCallHandlerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataInRef) throws IOException
    {
        MsgIntDrbdSetVlmUpToDate msg = MsgIntDrbdSetVlmUpToDate.parseDelimitedFrom(msgDataInRef);
        return vlmDfnInternalCallHandler.handleNotifySetUpToDateVlm(
            msg.getRscName(),
            msg.getVlmNrToUpToDateList()
        ).thenMany(Flux.empty());
    }
}
