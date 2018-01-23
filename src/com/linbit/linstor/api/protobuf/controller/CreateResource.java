package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.linbit.linstor.Volume.VlmApi;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgCrtRscOuterClass.MsgCrtRsc;
import com.linbit.linstor.proto.RscOuterClass.Rsc;
import com.linbit.linstor.proto.VlmOuterClass.Vlm;
import com.linbit.linstor.proto.apidata.VlmApiData;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class CreateResource extends BaseProtoApiCall
{
    private Controller controller;

    public CreateResource(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_RSC;
    }

    @Override
    public String getDescription()
    {
        return "Creates a resource from a resource definition and assigns it to a node";
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
        MsgCrtRsc msgCrtRsc = MsgCrtRsc.parseDelimitedFrom(msgDataIn);
        Rsc rsc = msgCrtRsc.getRsc();

        List<VlmApi> vlmApiDataList = new ArrayList<>();
        for (Vlm vlm : rsc.getVlmsList())
        {
            vlmApiDataList.add(new VlmApiData(vlm));
        }

        ApiCallRc apiCallRc = controller.getApiCallHandler().createResource(
            accCtx,
            client,
            rsc.getNodeName(),
            rsc.getName(),
            asMap(rsc.getPropsList()),
            vlmApiDataList
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
