package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlDrbdProxyEnableApiCallHandler;
import com.linbit.linstor.proto.MsgEnableDrbdProxyOuterClass.MsgEnableDrbdProxy;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_ENABLE_DRBD_PROXY,
    description = "Enables proxy on a resource connection"
)
@Singleton
public class EnableDrbdProxy implements ApiCallReactive
{
    private final CtrlDrbdProxyEnableApiCallHandler ctrlDrbdProxyEnableApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public EnableDrbdProxy(
        CtrlDrbdProxyEnableApiCallHandler ctrlDrbdProxyEnableApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlDrbdProxyEnableApiCallHandler = ctrlDrbdProxyEnableApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgEnableDrbdProxy msgEnableDrbdProxy = MsgEnableDrbdProxy.parseDelimitedFrom(msgDataIn);

        return ctrlDrbdProxyEnableApiCallHandler
            .enableProxy(
                msgEnableDrbdProxy.hasRscConnUuid() ? UUID.fromString(msgEnableDrbdProxy.getRscConnUuid()) : null,
                msgEnableDrbdProxy.getNode1Name(),
                msgEnableDrbdProxy.getNode2Name(),
                msgEnableDrbdProxy.getRscName(),
                msgEnableDrbdProxy.hasPort() ? msgEnableDrbdProxy.getPort() : null
            )
            .transform(responseSerializer::transform);
    }
}
