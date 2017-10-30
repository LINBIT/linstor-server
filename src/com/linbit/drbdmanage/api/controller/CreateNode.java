package com.linbit.drbdmanage.api.controller;

import java.io.IOException;
import java.io.InputStream;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.ApiConsts;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgCrtNodeOuterClass.MsgCrtNode;
import com.linbit.drbdmanage.security.AccessContext;

public class CreateNode extends BaseApiCall
{
    private final Controller controller;

    public CreateNode(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_NODE;
    }

    @Override
    public String getDescription()
    {
        return "Creates a node";
    }

    @Override
    public void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        MsgCrtNode msgCreateNode = MsgCrtNode.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().createNode(
            accCtx,
            client,
            msgCreateNode.getNodeName(),
            msgCreateNode.getNodeType(),
            msgCreateNode.getNodePropsMap()
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
