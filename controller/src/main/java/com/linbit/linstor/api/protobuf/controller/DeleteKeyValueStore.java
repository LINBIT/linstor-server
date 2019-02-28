package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.requests.MsgDelKvsOuterClass;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_DEL_KVS,
    description = "Deletes a KeyValueStore",
    transactional = true
)
@Singleton
public class DeleteKeyValueStore implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public DeleteKeyValueStore(
        CtrlApiCallHandler apiCallHandlerRef,
        ApiCallAnswerer apiCallAnswererRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgDelKvsOuterClass.MsgDelKvs msgDeleteKvs = MsgDelKvsOuterClass.MsgDelKvs.parseDelimitedFrom(msgDataIn);

        apiCallAnswerer.answerApiCallRc(
            apiCallHandler.deleteKvs(
                msgDeleteKvs.hasUuid() ? UUID.fromString(msgDeleteKvs.getUuid()) : null,
                msgDeleteKvs.getKvsName()
            )
        );
    }
}
