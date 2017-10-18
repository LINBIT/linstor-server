package com.linbit.drbdmanage.api.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgDelStorPoolDfnOuterClass.MsgDelStorPoolDfn;
import com.linbit.drbdmanage.security.AccessContext;

public class DeleteStorPoolDfn extends BaseApiCall
{
    private Controller controller;

    public DeleteStorPoolDfn(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_DEL_STOR_POOL_DFN;
    }

    @Override
    public String getDescription()
    {
        return "Deletes a storage pool definition";
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
        MsgDelStorPoolDfn msgDeleteStorPoolDfn = MsgDelStorPoolDfn.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().deleteStoragePoolDefinition(
            accCtx,
            client,
            msgDeleteStorPoolDfn.getStorPoolName()
        );

        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
