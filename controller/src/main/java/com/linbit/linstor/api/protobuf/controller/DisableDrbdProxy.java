package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlDrbdProxyDisableApiCallHandler;
import com.linbit.linstor.proto.MsgDisableDrbdProxyOuterClass.MsgDisableDrbdProxy;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_DISABLE_DRBD_PROXY,
    description = "Disables proxy on a resource connection"
)
public class DisableDrbdProxy implements ApiCallReactive
{
    private final CtrlDrbdProxyDisableApiCallHandler ctrlDrbdProxyDisableApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public DisableDrbdProxy(
        CtrlDrbdProxyDisableApiCallHandler ctrlDrbdProxyDisableApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlDrbdProxyDisableApiCallHandler = ctrlDrbdProxyDisableApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgDisableDrbdProxy msgDisableDrbdProxy = MsgDisableDrbdProxy.parseDelimitedFrom(msgDataIn);

        return ctrlDrbdProxyDisableApiCallHandler
            .disableProxy(
                msgDisableDrbdProxy.hasRscConnUuid() ? UUID.fromString(msgDisableDrbdProxy.getRscConnUuid()) : null,
                msgDisableDrbdProxy.getNode1Name(),
                msgDisableDrbdProxy.getNode2Name(),
                msgDisableDrbdProxy.getRscName()
            )
            .transform(responseSerializer::transform);
    }
}
