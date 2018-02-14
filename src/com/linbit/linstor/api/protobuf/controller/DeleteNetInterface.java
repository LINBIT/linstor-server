package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgDelNetInterfaceOuterClass.MsgDelNetInterface;
import com.linbit.linstor.security.AccessContext;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall
public class DeleteNetInterface extends BaseProtoApiCall
{
    private final Controller controller;

    public DeleteNetInterface(Controller ctrlRef)
    {
        super(ctrlRef.getErrorReporter());
        controller = ctrlRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_DEL_NET_IF;
    }

    @Override
    public String getDescription()
    {
        return "Deletes a network interface from a given node";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgDelNetInterface protoMsg = MsgDelNetInterface.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().deleteNetInterface(
            accCtx,
            client,
            protoMsg.getNodeName(),
            protoMsg.getNetIfName()
        );

        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
