package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlQueryMaxVlmSizeApiCallHandler;
import com.linbit.linstor.proto.MsgQryMaxVlmSizesOuterClass.MsgQryMaxVlmSizes;
import com.linbit.linstor.proto.apidata.AutoSelectFilterApiData;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_QRY_MAX_VLM_SIZE,
    description = "Queries the maximum volume size by given replica-count"
)
@Singleton
public class MaxVlmSize implements ApiCallReactive
{
    private final CtrlQueryMaxVlmSizeApiCallHandler ctrlQueryMaxVlmSizeApiCallHandler;

    @Inject
    public MaxVlmSize(CtrlQueryMaxVlmSizeApiCallHandler ctrlQueryMaxVlmSizeApiCallHandlerRef)
    {
        ctrlQueryMaxVlmSizeApiCallHandler = ctrlQueryMaxVlmSizeApiCallHandlerRef;
    }


    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgQryMaxVlmSizes msgQuery = MsgQryMaxVlmSizes.parseDelimitedFrom(msgDataIn);
        return ctrlQueryMaxVlmSizeApiCallHandler.queryMaxVlmSize(
            new AutoSelectFilterApiData(msgQuery.getSelectFilter())
        );
    }
}
