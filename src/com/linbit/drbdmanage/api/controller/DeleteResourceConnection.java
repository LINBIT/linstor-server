package com.linbit.drbdmanage.api.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.ApiConsts;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgDelRscConnOuterClass.MsgDelRscConn;
import com.linbit.drbdmanage.security.AccessContext;

public class DeleteResourceConnection extends BaseApiCall
{
    private final Controller controller;

    public DeleteResourceConnection(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_DEL_RSC_CONN;
    }

    @Override
    public String getDescription()
    {
        return "Deletes resource connection options";
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
        MsgDelRscConn msgDeleteRscConn = MsgDelRscConn.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().deleteResourceConnection(
            accCtx,
            client,
            msgDeleteRscConn.getNodeName1(),
            msgDeleteRscConn.getNodeName2(),
            msgDeleteRscConn.getResourceName()
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
