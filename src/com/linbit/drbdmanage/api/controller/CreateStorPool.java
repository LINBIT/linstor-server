package com.linbit.drbdmanage.api.controller;

import java.io.IOException;
import java.io.InputStream;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgCrtStorPoolOuterClass.MsgCrtStorPool;
import com.linbit.drbdmanage.security.AccessContext;

public class CreateStorPool extends BaseApiCall
{
    private Controller controller;

    public CreateStorPool(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_STOR_POOL;
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
        MsgCrtStorPool msgCreateStorPool= MsgCrtStorPool.parseDelimitedFrom(msgDataIn);

        ApiCallRc apiCallRc = controller.getApiCallHandler().createStoragePool(
            accCtx,
            client,
            msgCreateStorPool.getNodeName(),
            msgCreateStorPool.getStorPoolName(),
            msgCreateStorPool.getDriver(),
            msgCreateStorPool.getStorPoolPropsMap()
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
