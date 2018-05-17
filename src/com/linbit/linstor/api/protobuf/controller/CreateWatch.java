package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgCrtWatchOuterClass;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_WATCH,
    description = "Creates a watch"
)
public class CreateWatch implements ApiCall
{
    private final ApiCallAnswerer apiCallAnswerer;
    private final CtrlApiCallHandler ctrlApiCallHandler;

    @Inject
    public CreateWatch(
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
        MsgCrtWatchOuterClass.MsgCrtWatch msgWatch =
            MsgCrtWatchOuterClass.MsgCrtWatch.parseDelimitedFrom(msgDataIn);

        ApiCallRc apiCallRc = ctrlApiCallHandler.createWatch(msgWatch.getWatchId(),
            msgWatch.getEventName(),
            msgWatch.hasNodeName() ? msgWatch.getNodeName() : null,
            msgWatch.hasResourceName() ? msgWatch.getResourceName() : null,
            msgWatch.getFilterByVolumeNumber() ? msgWatch.getVolumeNumber() : null,
            msgWatch.hasSnapshotName() ? msgWatch.getSnapshotName() : null
        );

        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
