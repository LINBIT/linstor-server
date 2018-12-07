package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolCrtApiCallHandler;
import com.linbit.linstor.proto.MsgCrtStorPoolOuterClass.MsgCrtStorPool;
import com.linbit.linstor.proto.StorPoolOuterClass.StorPool;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_STOR_POOL,
    description = "Creates a storage pool name registration"
)
@Singleton
public class CreateStorPool implements ApiCallReactive
{
    private final CtrlStorPoolCrtApiCallHandler ctrlStorPoolCrtApiCallHandler;
    private final ResponseSerializer responseSerializer;

    @Inject
    public CreateStorPool(
        CtrlStorPoolCrtApiCallHandler ctrlStorPoolCrtApiCallHandlerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        ctrlStorPoolCrtApiCallHandler = ctrlStorPoolCrtApiCallHandlerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtStorPool msgCreateStorPool = MsgCrtStorPool.parseDelimitedFrom(msgDataIn);
        StorPool storPool = msgCreateStorPool.getStorPool();

        return ctrlStorPoolCrtApiCallHandler
            .createStorPool(
                storPool.getNodeName(),
                storPool.getStorPoolName(),
                storPool.getDriver(),
                storPool.getFreeSpaceMgrName(),
                ProtoMapUtils.asMap(storPool.getPropsList())
            )
            .transform(responseSerializer::transform);
    }

}
