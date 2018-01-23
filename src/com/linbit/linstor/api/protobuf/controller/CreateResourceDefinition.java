package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgCrtRscDfnOuterClass.MsgCrtRscDfn;
import com.linbit.linstor.proto.RscDfnOuterClass.RscDfn;
import com.linbit.linstor.proto.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.apidata.VlmDfnApiData;
import com.linbit.linstor.security.AccessContext;

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
        RscDfn rscDfn = msgCreateRscDfn.getRscDfn();

        List<VlmDfnApi> vlmDfnApiList = new ArrayList<>();
        for (final VlmDfn vlmDfn : rscDfn.getVlmDfnsList())
        {
            vlmDfnApiList.add(new VlmDfnApiData(vlmDfn));
        }

        ApiCallRc apiCallRc = controller.getApiCallHandler().createResourceDefinition(
            accCtx,
            client,
            rscDfn.getRscName(),
            rscDfn.hasRscDfnPort() ? rscDfn.getRscDfnPort() : null,
            rscDfn.getRscDfnSecret(),
            rscDfn.getRscDfnTransportType(),
            asMap(rscDfn.getRscDfnPropsList()),
            vlmDfnApiList
        );
        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
