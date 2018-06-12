package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgRestoreSnapshotVlmDfnOuterClass.MsgRestoreSnapshotVlmDfn;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_RESTORE_VLM_DFN,
    description = "Creates volume definitions from a snapshot"
)
public class RestoreVolumeDefinition implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public RestoreVolumeDefinition(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgRestoreSnapshotVlmDfn msgRestoreSnapshotVlmDfn = MsgRestoreSnapshotVlmDfn.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = apiCallHandler.restoreVlmDfn(
            msgRestoreSnapshotVlmDfn.getFromResourceName(),
            msgRestoreSnapshotVlmDfn.getFromSnapshotName(),
            msgRestoreSnapshotVlmDfn.getToResourceName()
        );

        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
