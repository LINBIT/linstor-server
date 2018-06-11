package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgRestoreSnapshotRscOuterClass;
import com.linbit.linstor.proto.MsgRestoreSnapshotRscOuterClass.MsgRestoreSnapshotRsc;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Collectors;

@ProtobufApiCall(
    name = ApiConsts.API_RESTORE_SNAPSHOT,
    description = "Restores a snapshot"
)
public class RestoreSnapshot implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public RestoreSnapshot(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgRestoreSnapshotRsc msgRestoreSnapshotRsc = MsgRestoreSnapshotRsc.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = apiCallHandler.restoreSnapshot(
            msgRestoreSnapshotRsc.getNodesList().stream()
                .map(MsgRestoreSnapshotRscOuterClass.RestoreNode::getName)
                .collect(Collectors.toList()),
            msgRestoreSnapshotRsc.getFromResourceName(),
            msgRestoreSnapshotRsc.getFromSnapshotName(),
            msgRestoreSnapshotRsc.getToResourceName()
        );

        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
