package com.linbit.drbdmanage.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.drbdmanage.api.ApiCallRc;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgCrtStorPoolOuterClass.MsgCrtStorPool;
import com.linbit.drbdmanage.security.AccessContext;

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

        ApiCallRc apiCallRc = controller.getApiCallHandler().createStoragePool(
            accCtx,
            client,
            msgCreateStorPool.getNodeName(),
            msgCreateStorPool.getStorPoolName(),
            msgCreateStorPool.getDriver(),
            asMap(msgCreateStorPool.getStorPoolPropsList())
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
