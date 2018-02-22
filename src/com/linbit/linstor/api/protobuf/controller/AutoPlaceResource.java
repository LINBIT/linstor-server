package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;

import com.google.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgAutoPlaceRscOuterClass.MsgAutoPlaceRsc;

@ProtobufApiCall(
    name = ApiConsts.API_AUTO_PLACE_RSC,
    description = "Creates a resource from a resource definition and assigns it to a node"
)
public class AutoPlaceResource implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public AutoPlaceResource(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgAutoPlaceRsc msgAutoPlace = MsgAutoPlaceRsc.parseDelimitedFrom(msgDataIn);

        System.out.println("AUTOPLACE message:\n" + msgAutoPlace.toString());

        // TODO implement
        ApiCallRcImpl rc = new ApiCallRcImpl();
        ApiCallRcImpl.ApiCallRcEntry rci = new ApiCallRcImpl.ApiCallRcEntry();
        rci.setMessageFormat("Not implemented yet.");
        rci.setReturnCode(ApiConsts.MASK_ERROR | ApiConsts.FAIL_IMPL_ERROR);
        rc.addEntry(rci);
        apiCallAnswerer.answerApiCallRc(rc);
    }
}
