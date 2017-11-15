package com.linbit.drbdmanage.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.linbit.drbdmanage.VolumeDefinition.VlmDfnApi;
import com.linbit.drbdmanage.api.ApiCallRc;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgCrtRscDfnOuterClass.MsgCrtRscDfn;
import com.linbit.drbdmanage.proto.MsgCrtVlmDfnOuterClass.VlmDfn;
import com.linbit.drbdmanage.proto.apidata.VlmDfnApiData;
import com.linbit.drbdmanage.security.AccessContext;

@ProtobufApiCall
public class CreateResourceDefinition extends BaseProtoApiCall
{
    private final Controller controller;

    public CreateResourceDefinition(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_RSC_DFN;
    }

    @Override
    public String getDescription()
    {
        return "Creates a resource definition";
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
        MsgCrtRscDfn msgCreateRscDfn = MsgCrtRscDfn.parseDelimitedFrom(msgDataIn);

        List<VlmDfnApi> vlmDfnApiList = new ArrayList<>();
        for (final VlmDfn vlmDfn : msgCreateRscDfn.getVlmDfnsList())
        {
            vlmDfnApiList.add(new VlmDfnApiData(vlmDfn));
        }

        ApiCallRc apiCallRc = controller.getApiCallHandler().createResourceDefinition(
            accCtx,
            client,
            msgCreateRscDfn.getRscName(),
            msgCreateRscDfn.getRscPort(),
            msgCreateRscDfn.getRscSecret(),
            asMap(msgCreateRscDfn.getRscPropsList()),
            vlmDfnApiList
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
