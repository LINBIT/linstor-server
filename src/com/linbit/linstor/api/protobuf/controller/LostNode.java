package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgDelNodeOuterClass.MsgDelNode;

@ProtobufApiCall(
    name = ApiConsts.API_LOST_NODE,
    description = "Deletes a node that will never appear again"
)
public class LostNode implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public LostNode(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgDelNode msgDeleteNode = MsgDelNode.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = apiCallHandler.lostNode(
            msgDeleteNode.getNodeName()
        );

        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
