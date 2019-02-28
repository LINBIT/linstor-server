package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoPlaceApiCallHandler;
import com.linbit.linstor.proto.apidata.AutoSelectFilterApiData;
import com.linbit.linstor.proto.requests.MsgAutoPlaceRscOuterClass.MsgAutoPlaceRsc;

import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_AUTO_PLACE_RSC,
    description = "Creates a resource from a resource definition and assigns it to a node"
)
@Singleton
public class AutoPlaceResource implements ApiCallReactive
{
    private final CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public AutoPlaceResource(
        CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlRscAutoPlaceApiCallHandler = ctrlRscAutoPlaceApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgAutoPlaceRsc msgAutoPlace = MsgAutoPlaceRsc.parseDelimitedFrom(msgDataIn);
        AutoSelectFilterApi filter = new AutoSelectFilterApiData(msgAutoPlace.getSelectFilter());

        return ctrlRscAutoPlaceApiCallHandler
            .autoPlace(
                msgAutoPlace.getRscName(),
                filter,
                msgAutoPlace.hasDisklessOnRemaining() ? msgAutoPlace.getDisklessOnRemaining() : false
            )
            .transform(responseSerializer::transform);
    }
}
