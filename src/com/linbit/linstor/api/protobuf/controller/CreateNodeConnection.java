package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgCrtNodeConnOuterClass.MsgCrtNodeConn;
import com.linbit.linstor.proto.NodeConnOuterClass.NodeConn;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_NODE_CONN,
    description = "Defines node connection options"
)
public class CreateNodeConnection implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public CreateNodeConnection(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtNodeConn msgCreateNodeConn = MsgCrtNodeConn.parseDelimitedFrom(msgDataIn);
        NodeConn nodeConn = msgCreateNodeConn.getNodeConn();
        ApiCallRc apiCallRc = apiCallHandler.createNodeConnection(
            // ignore nodeConnUuid
            nodeConn.getNodeName1(),
            nodeConn.getNodeName2(),
            ProtoMapUtils.asMap(nodeConn.getNodeConnPropsList())
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
