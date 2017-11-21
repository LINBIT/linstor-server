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
import com.linbit.drbdmanage.proto.MsgCrtVlmDfnOuterClass.MsgCrtVlmDfn;
import com.linbit.drbdmanage.proto.VlmDfnOuterClass.VlmDfn;
import com.linbit.drbdmanage.proto.apidata.VlmDfnApiData;
import com.linbit.drbdmanage.security.AccessContext;

@ProtobufApiCall
public class CreateVolumeDefinition extends BaseProtoApiCall
{
    private Controller controller;

    public CreateVolumeDefinition(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_VLM_DFN;
    }

    @Override
    public String getDescription()
    {
        return "Creates a volume definition";
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
        MsgCrtVlmDfn msgCrtVlmDfn = MsgCrtVlmDfn.parseDelimitedFrom(msgDataIn);

        List<VlmDfnApi> vlmDfnApiList = new ArrayList<>();
        for (final VlmDfn vlmDfn : msgCrtVlmDfn.getVlmDfnsList())
        {
            vlmDfnApiList.add(new VlmDfnApiData(vlmDfn));
        }
        ApiCallRc apiCallRc = controller.getApiCallHandler().createVlmDfns(
            accCtx,
            client,
            msgCrtVlmDfn.getRscName(),
            vlmDfnApiList
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
