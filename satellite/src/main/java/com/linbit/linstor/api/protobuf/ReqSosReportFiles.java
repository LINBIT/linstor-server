package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.core.apicallhandler.StltSosReportApiCallHandler;
import com.linbit.linstor.proto.requests.MsgReqSosReportFilesOuterClass.MsgReqSosReportFiles;

import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

import com.google.inject.Inject;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQ_SOS_REPORT_FILES,
    description = "Returns the requested files of the specified SOS report.",
    transactional = false
)
@Singleton
public class ReqSosReportFiles implements ApiCall
{
    private final StltSosReportApiCallHandler sosApiCallHandler;

    @Inject
    public ReqSosReportFiles(StltSosReportApiCallHandler sosApiCallHandlerRef)
    {
        sosApiCallHandler = sosApiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgReqSosReportFiles reqSosReportFiles = MsgReqSosReportFiles.parseDelimitedFrom(msgDataIn);
        sosApiCallHandler.handleSosReportRequestedFiles(
            reqSosReportFiles.getSosReportName(),
            ProtoDeserializationUtils.parseRequestedSosFiles(reqSosReportFiles.getFilesList())
        );

    }
}
