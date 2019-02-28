package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeLostApiCallHandler;
import com.linbit.linstor.proto.requests.MsgDelNodeOuterClass.MsgDelNode;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_LOST_NODE,
    description = "Deletes a node that will never appear again"
)
@Singleton
public class LostNode implements ApiCallReactive
{
    private final CtrlNodeLostApiCallHandler ctrlNodeLostApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public LostNode(
        CtrlNodeLostApiCallHandler ctrlNodeLostApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlNodeLostApiCallHandler = ctrlNodeLostApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgDelNode msgDeleteNode = MsgDelNode.parseDelimitedFrom(msgDataIn);
        return ctrlNodeLostApiCallHandler
            .lostNode(msgDeleteNode.getNodeName())
            .transform(responseSerializer::transform);
    }
}
