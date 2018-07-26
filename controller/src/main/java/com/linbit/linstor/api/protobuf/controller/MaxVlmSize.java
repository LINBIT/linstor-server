package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgQryMaxVlmSizesOuterClass.MsgQryMaxVlmSizes;
import com.linbit.linstor.proto.apidata.AutoSelectFilterApiData;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_QRY_MAX_VLM_SIZE,
    description = "Queries the maximum volume size by given replica-count"
)
public class MaxVlmSize implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Peer client;

    @Inject
    public MaxVlmSize(CtrlApiCallHandler apiCallHandlerRef, Peer clientRef)
    {
        apiCallHandler = apiCallHandlerRef;
        client = clientRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgQryMaxVlmSizes msgQuery = MsgQryMaxVlmSizes.parseDelimitedFrom(msgDataIn);
        client.sendMessage(
            apiCallHandler.queryMaxVlmSize(
                new AutoSelectFilterApiData(
                    msgQuery.getSelectFilter()
                )
            )
        );
    }
}
