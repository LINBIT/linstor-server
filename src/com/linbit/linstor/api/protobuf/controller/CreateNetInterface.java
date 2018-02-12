package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgCrtNetInterfaceOuterClass.MsgCrtNetInterface;
import com.linbit.linstor.proto.NetInterfaceOuterClass;
import com.linbit.linstor.security.AccessContext;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall
public class CreateNetInterface extends BaseProtoApiCall
{
    private Controller controller;

    public CreateNetInterface(Controller ctrlRef)
    {
        super(ctrlRef.getErrorReporter());
        controller = ctrlRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_NET_IF;
    }

    @Override
    public String getDescription()
    {
        return "Creates a new network interface for a given node";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgCrtNetInterface protoMsg = MsgCrtNetInterface.parseDelimitedFrom(msgDataIn);

        NetInterfaceOuterClass.NetInterface netIf = protoMsg.getNetIf();
        ApiCallRc apiCallRc = controller.getApiCallHandler().createNetInterface(
            accCtx,
            client,
            protoMsg.getNodeName(),
            netIf.getName(),
            netIf.getAddress()
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
