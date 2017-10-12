package com.linbit.drbdmanage.api.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgCrtNodeConnOuterClass.MsgCrtNodeConn;
import com.linbit.drbdmanage.security.AccessContext;

public class CreateNodeConnection extends BaseApiCall
{
    private final Controller controller;

    public CreateNodeConnection(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_NODE_CONN;
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        MsgCrtNodeConn msgCreateNodeConn = MsgCrtNodeConn.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().createNodeConnection(
            accCtx,
            client,
            msgCreateNodeConn.getNodeName1(),
            msgCreateNodeConn.getNodeName2(),
            msgCreateNodeConn.getNodeConnPropsMap()
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
