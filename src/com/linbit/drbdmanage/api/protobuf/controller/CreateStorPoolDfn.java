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
import com.linbit.drbdmanage.proto.MsgCrtStorPoolDfnOuterClass.MsgCrtStorPoolDfn;
import com.linbit.drbdmanage.security.AccessContext;

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

        ApiCallRc apiCallRc = controller.getApiCallHandler().createStoragePoolDefinition(
            accCtx,
            client,
            msgCreateStorPoolDfn.getStorPoolName(),
            asMap(msgCreateStorPoolDfn.getStorPoolDfnPropsList())
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
