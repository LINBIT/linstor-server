package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgModNetInterfaceOuterClass.MsgModNetInterface;
import com.linbit.linstor.proto.NetInterfaceOuterClass.NetInterface;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_MOD_NET_IF,
    description = "Modifies a network interface of a given node"
)
public class ModifyNetInterface implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyNetInterface(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
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

        ApiCallRc apiCallRc = apiCallHandler.modifyNetInterface(
            nodeName,
            netIfName,
            protoNetIf.hasAddress() ? protoNetIf.getAddress() : null,
            protoNetIf.hasStltPort() ? protoNetIf.getStltPort() : null,
            protoNetIf.hasStltEncryptionType() ? protoNetIf.getStltEncryptionType() : null
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
