package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.requests.MsgNodeReconnectOuterClass.MsgNodeReconnect;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_NODE_RECONNECT,
    description = "Reconnects a node",
    transactional = false
)
@Singleton
public class NodeReconnect implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public NodeReconnect(
        CtrlApiCallHandler apiCallHandlerRef,
        ApiCallAnswerer apiCallAnswererRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgNodeReconnect msgNodeReconnect = MsgNodeReconnect.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = apiCallHandler.reconnectNode(msgNodeReconnect.getNodesList());
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
