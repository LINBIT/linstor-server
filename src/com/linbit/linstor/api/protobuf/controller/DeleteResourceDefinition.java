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
import com.linbit.linstor.proto.MsgDelRscDfnOuterClass.MsgDelRscDfn;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class DeleteResourceDefinition extends BaseProtoApiCall
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
