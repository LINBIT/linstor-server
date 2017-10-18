package com.linbit.drbdmanage.api.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgDelVlmConnOuterClass.MsgDelVlmConn;
import com.linbit.drbdmanage.security.AccessContext;

public class DeleteVolumeConnection extends BaseApiCall
{
    private final Controller controller;

    public DeleteVolumeConnection(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_DEL_VLM_CONN;
    }

    @Override
    public String getDescription()
    {
        return "Deletes volume connection options";
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
        MsgDelVlmConn msgDeleteVlmConn = MsgDelVlmConn.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().deleteVolumeConnection(
            accCtx,
            client,
            msgDeleteVlmConn.getNodeName1(),
            msgDeleteVlmConn.getNodeName2(),
            msgDeleteVlmConn.getResourceName(),
            msgDeleteVlmConn.getVolumeNr()
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
