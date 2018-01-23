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
import com.linbit.linstor.proto.MsgCrtStorPoolOuterClass.MsgCrtStorPool;
import com.linbit.linstor.proto.StorPoolOuterClass.StorPool;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class CreateStorPool extends BaseProtoApiCall
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
    public String getDescription()
    {
        return "Creates a storage pool name registration";
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
        StorPool storPool = msgCreateStorPool.getStorPool();

        ApiCallRc apiCallRc = controller.getApiCallHandler().createStoragePool(
            accCtx,
            client,
            storPool.getNodeName(),
            storPool.getStorPoolName(),
            storPool.getDriver(),
            asMap(storPool.getPropsList())
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
