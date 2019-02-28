package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.requests.MsgCrtSfTargetNodeOuterClass.MsgCrtSfTargetNode;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_SF_TARGET_NODE,
    description = "Creates a swordfish target node"
)
@Singleton
public class CreateSwordfishTargetNode implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public CreateSwordfishTargetNode(
        CtrlApiCallHandler apiCallHandlerRef,
        ApiCallAnswerer apiCallAnswererRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtSfTargetNode msgCreateSfNode = MsgCrtSfTargetNode.parseDelimitedFrom(msgDataIn);
        String nodeName = msgCreateSfNode.getName();
        ApiCallRc apiCallRc = apiCallHandler.createSwordfishTargetNode(
            nodeName,
            ProtoMapUtils.asMap(msgCreateSfNode.getPropsList())
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
