package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgCrtWatchOuterClass;
import com.linbit.linstor.proto.MsgDelWatchOuterClass;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_DEL_WATCH,
    description = "Deletes a watch"
)
public class DeleteWatch implements ApiCall
{
    private final ApiCallAnswerer apiCallAnswerer;
    private final CtrlApiCallHandler ctrlApiCallHandler;

    @Inject
    public DeleteWatch(
        ApiCallAnswerer apiCallAnswererRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef
    )
    {
        apiCallAnswerer = apiCallAnswererRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgDelWatchOuterClass.MsgDelWatch msgDelWatch =
            MsgDelWatchOuterClass.MsgDelWatch.parseDelimitedFrom(msgDataIn);

        ApiCallRc apiCallRc = ctrlApiCallHandler.deleteWatch(msgDelWatch.getWatchId());

        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
