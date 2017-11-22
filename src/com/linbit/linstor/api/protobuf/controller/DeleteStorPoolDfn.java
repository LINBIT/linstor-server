package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgDelStorPoolDfnOuterClass.MsgDelStorPoolDfn;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class DeleteStorPoolDfn extends BaseProtoApiCall
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
