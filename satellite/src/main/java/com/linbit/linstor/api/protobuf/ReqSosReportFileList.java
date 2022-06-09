package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.core.apicallhandler.StltSosReportApiCallHandler;
import com.linbit.linstor.proto.requests.MsgReqSosReportListOuterClass.MsgReqSosReportList;

import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import com.google.inject.Inject;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQ_SOS_REPORT_FILE_LIST,
    description = "Prepares the sos report and returns a file list (no content).",
    transactional = false
)
@Singleton
public class ReqSosReportFileList implements ApiCall
{
    private final StltSosReportApiCallHandler sosApiCallHandler;

    @Inject
    public ReqSosReportFileList(StltSosReportApiCallHandler sosApiCallHandlerRef)
    {
        sosApiCallHandler = sosApiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgReqSosReportList reqSosReport = MsgReqSosReportList.parseDelimitedFrom(msgDataIn);
        sosApiCallHandler.handleSosReportRequestFileList(
            reqSosReport.getSosReportName(),
            new Date(reqSosReport.getSince())
        );
    }
}
