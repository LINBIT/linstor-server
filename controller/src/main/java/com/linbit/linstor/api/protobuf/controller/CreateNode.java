package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeCrtApiCallHandler;
import com.linbit.linstor.proto.requests.MsgCrtNodeOuterClass.MsgCrtNode;
import com.linbit.linstor.proto.common.NetInterfaceOuterClass;
import com.linbit.linstor.proto.common.NodeOuterClass;
import com.linbit.linstor.proto.apidata.NetInterfaceApiData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_NODE,
    description = "Creates a node",
    transactional = true
)
@Singleton
public class CreateNode implements ApiCallReactive
{
    private final CtrlNodeCrtApiCallHandler nodeCrtApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public CreateNode(
        CtrlNodeCrtApiCallHandler nodeCrtApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        nodeCrtApiCallHandler = nodeCrtApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtNode msgCreateNode = MsgCrtNode.parseDelimitedFrom(msgDataIn);
        NodeOuterClass.Node protoNode = msgCreateNode.getNode();

        return nodeCrtApiCallHandler.createNode(
            protoNode.getName(),
            protoNode.getType(),
            extractNetIfs(protoNode.getNetInterfacesList()),
            ProtoMapUtils.asMap(protoNode.getPropsList())
        ).transform(responseSerializer::transform);
    }

    private List<NetInterfaceApi> extractNetIfs(List<NetInterfaceOuterClass.NetInterface> protoNetIfs)
    {
        List<NetInterfaceApi> netIfs = new ArrayList<>();
        for (NetInterfaceOuterClass.NetInterface protoNetIf : protoNetIfs)
        {
            netIfs.add(new NetInterfaceApiData(protoNetIf));
        }
        return netIfs;
    }
}
