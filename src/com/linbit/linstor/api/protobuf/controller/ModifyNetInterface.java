package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgModNetInterfaceOuterClass.MsgModNetInterface;
import com.linbit.linstor.proto.NetInterfaceOuterClass.NetInterface;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ModifyNetInterface extends BaseProtoApiCall
{
    private final Controller controller;

    public ModifyNetInterface(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_MOD_NET_IF;
    }

    @Override
    public String getDescription()
    {
        return "Modifies a network interface of a given node";
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
        MsgModNetInterface msgModNetIf = MsgModNetInterface.parseDelimitedFrom(msgDataIn);
        UUID netIfUuid = null;
        NetInterface protoNetIf = msgModNetIf.getNetIf();
        if (protoNetIf.hasUuid())
        {
            netIfUuid = UUID.fromString(protoNetIf.getUuid());
        }
        String nodeName = msgModNetIf.getNodeName();
        String netIfName = protoNetIf.getName();
        String addr = null;
        if (protoNetIf.hasAddress())
        {
            addr = protoNetIf.getAddress();
        }

        ApiCallRc apiCallRc = controller.getApiCallHandler().modifyNetInterface(
            accCtx,
            client,
            nodeName,
            netIfName,
            addr
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
