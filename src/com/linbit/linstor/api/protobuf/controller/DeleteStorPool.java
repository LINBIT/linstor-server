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
import com.linbit.linstor.proto.MsgDelStorPoolOuterClass.MsgDelStorPool;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class DeleteStorPool extends BaseProtoApiCall
{
    private Controller controller;

    public DeleteStorPool(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_DEL_STOR_POOL;
    }

    @Override
    public String getDescription()
    {
        return "Deletes a storage pool name registration";
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
        MsgDelStorPool msgDeleteStorPool= MsgDelStorPool.parseDelimitedFrom(msgDataIn);

        ApiCallRc apiCallRc = controller.getApiCallHandler().deleteStoragePool(
            accCtx,
            client,
            msgDeleteStorPool.getNodeName(),
            msgDeleteStorPool.getStorPoolName()
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
