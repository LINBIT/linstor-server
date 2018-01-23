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
import com.linbit.linstor.proto.MsgCrtStorPoolDfnOuterClass.MsgCrtStorPoolDfn;
import com.linbit.linstor.proto.StorPoolDfnOuterClass.StorPoolDfn;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class CreateStorPoolDfn extends BaseProtoApiCall
{

    private Controller controller;

    public CreateStorPoolDfn(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_STOR_POOL_DFN;
    }

    @Override
    public String getDescription()
    {
        return "Creates a storage pool definition";
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
        MsgCrtStorPoolDfn msgCreateStorPoolDfn = MsgCrtStorPoolDfn.parseDelimitedFrom(msgDataIn);
        StorPoolDfn storPoolDfn = msgCreateStorPoolDfn.getStorPoolDfn();

        ApiCallRc apiCallRc = controller.getApiCallHandler().createStoragePoolDefinition(
            accCtx,
            client,
            storPoolDfn.getStorPoolName(),
            asMap(storPoolDfn.getPropsList())
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
