package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgCrtNetInterfaceOuterClass.MsgCrtNetInterface;
import com.linbit.linstor.proto.NetInterfaceOuterClass;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_NET_IF,
    description = "Creates a new network interface for a given node"
)
public class CreateNetInterface implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public CreateNetInterface(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtNetInterface protoMsg = MsgCrtNetInterface.parseDelimitedFrom(msgDataIn);

        NetInterfaceOuterClass.NetInterface netIf = protoMsg.getNetIf();
        ApiCallRc apiCallRc = apiCallHandler.createNetInterface(
            protoMsg.getNodeName(),
            netIf.getName(),
            netIf.getAddress(),
            netIf.hasStltPort() ? netIf.getStltPort() : null,
            netIf.hasStltEncryptionType() ? netIf.getStltEncryptionType() : null
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
