package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgDelVlmDfnOuterClass.MsgDelVlmDfn;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_DEL_VLM_DFN,
    description = "Deletes a volume definition"
)
public class DeleteVolumeDefinition implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public DeleteVolumeDefinition(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgDelVlmDfn msgDelVlmDfn = MsgDelVlmDfn.parseDelimitedFrom(msgDataIn);

        ApiCallRc apiCallRc = apiCallHandler.deleteVolumeDefinition(
                msgDelVlmDfn.getRscName(),
                msgDelVlmDfn.getVlmNr()
        );

        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
