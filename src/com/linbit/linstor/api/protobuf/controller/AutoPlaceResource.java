package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;

import javax.inject.Inject;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
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

        ApiCallRc rc = apiCallHandler.createResourcesAutoPlace(
            msgAutoPlace.getRscName(),
            msgAutoPlace.getPlaceCount(),
            msgAutoPlace.hasStoragePool() ? msgAutoPlace.getStoragePool() : null,
            msgAutoPlace.getNotPlaceWithRscList(),
            msgAutoPlace.hasNotPlaceWithRscRegex() ? msgAutoPlace.getNotPlaceWithRscRegex() : null,
            msgAutoPlace.getReplicasOnDifferentList(),
            msgAutoPlace.getReplicasOnSameList()
        );
        apiCallAnswerer.answerApiCallRc(rc);
    }
}
