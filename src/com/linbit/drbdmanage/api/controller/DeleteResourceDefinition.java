package com.linbit.drbdmanage.api.controller;

import java.io.IOException;
import java.io.InputStream;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgDelRscDfnOuterClass.MsgDelRscDfn;
import com.linbit.drbdmanage.security.AccessContext;

public class DeleteResourceDefinition extends BaseApiCall
{
    private final Controller controller;

    public DeleteResourceDefinition(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_DEL_RSC_DFN;
    }

    @Override
    public String getDescription()
    {
        return "Deletes a resource definition";
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
        MsgDelRscDfn msgDeleteRscDfn = MsgDelRscDfn.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().deleteResourceDefinition(
            accCtx,
            client,
            msgDeleteRscDfn.getRscName()
        );

        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
