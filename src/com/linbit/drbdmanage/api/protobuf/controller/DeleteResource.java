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
import com.linbit.drbdmanage.proto.MsgDelRscOuterClass.MsgDelRsc;
import com.linbit.drbdmanage.security.AccessContext;

@ProtobufApiCall
public class DeleteResource extends BaseProtoApiCall
{
    private final Controller controller;

    public DeleteResource(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_DEL_RSC;
    }

    @Override
    public String getDescription()
    {
        return "Deletes a resource";
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
        MsgDelRsc msgDeleteRsc = MsgDelRsc.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().deleteResource(
            accCtx,
            client,
            msgDeleteRsc.getNodeName(),
            msgDeleteRsc.getRscName()
        );

        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
