package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeDeleteApiCallHandler;
import com.linbit.linstor.proto.MsgDelNodeOuterClass.MsgDelNode;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_DEL_NODE,
    description = "Deletes a node"
)
@Singleton
public class DeleteNode implements ApiCallReactive
{
    private final CtrlNodeDeleteApiCallHandler ctrlNodeDeleteApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public DeleteNode(
        CtrlNodeDeleteApiCallHandler ctrlNodeDeleteApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlNodeDeleteApiCallHandler = ctrlNodeDeleteApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgDelNode msgDeleteNode = MsgDelNode.parseDelimitedFrom(msgDataIn);
        return ctrlNodeDeleteApiCallHandler
            .deleteNode(msgDeleteNode.getNodeName())
            .transform(responseSerializer::transform);
    }
}
