package com.linbit.drbdmanage.api.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.ApiConsts;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgCrtRscConnOuterClass.MsgCrtRscConn;
import com.linbit.drbdmanage.security.AccessContext;

public class CreateResourceConnection extends BaseApiCall
{
    private final Controller controller;

    public CreateResourceConnection(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_RSC_CONN;
    }

    @Override
    public String getDescription()
    {
        return "Defines resource connection options";
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
        MsgCrtRscConn msgCreateRscConn = MsgCrtRscConn.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().createResourceConnection(
            accCtx,
            client,
            msgCreateRscConn.getNodeName1(),
            msgCreateRscConn.getNodeName2(),
            msgCreateRscConn.getResourceName(),
            msgCreateRscConn.getResourceConnPropsMap()
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
