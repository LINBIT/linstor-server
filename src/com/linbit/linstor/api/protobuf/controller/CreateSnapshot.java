package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgCrtSnapshotOuterClass.MsgCrtSnapshot;
import com.linbit.linstor.proto.SnapshotDfnOuterClass.SnapshotDfn;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_SNAPSHOT,
    description = "Creates a snapshot"
)
public class CreateSnapshot implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public CreateSnapshot(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgCrtSnapshot msgCrtRsc = MsgCrtSnapshot.parseDelimitedFrom(msgDataIn);
        SnapshotDfn snapshotDfn = msgCrtRsc.getSnapshotDfn();

        ApiCallRc apiCallRc = apiCallHandler.createSnapshot(
            snapshotDfn.getRscName(),
            snapshotDfn.getSnapshotName()
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
