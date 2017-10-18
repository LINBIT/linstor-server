package com.linbit.drbdmanage.api.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.Volume.VlmApi;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgCrtRscOuterClass.MsgCrtRsc;
import com.linbit.drbdmanage.proto.MsgCrtRscOuterClass.Vlm;
import com.linbit.drbdmanage.proto.apidata.VlmApiData;
import com.linbit.drbdmanage.security.AccessContext;

public class CreateResource extends BaseApiCall
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

        List<VlmApi> vlmApiDataList = new ArrayList<>();
        for (Vlm vlm : msgCrtRsc.getVlmsList())
        {
            vlmApiDataList.add(new VlmApiData(vlm));
        }

        ApiCallRc apiCallRc = controller.getApiCallHandler().createResource(
            accCtx,
            client,
            msgCrtRsc.getNodeName(),
            msgCrtRsc.getRscName(),
            msgCrtRsc.getNodeId(),
            msgCrtRsc.getRscPropsMap(),
            vlmApiDataList
        );

        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
