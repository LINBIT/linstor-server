package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.StltSosReportApiCallHandler;
import com.linbit.linstor.proto.requests.MsgReqSosReportOuterClass.MsgReqSosReport;

import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import com.google.inject.Inject;

@ProtobufApiCall(
    name = ApiConsts.API_REQ_SOS_REPORT,
    description = "Returns the requested SOS report.",
    transactional = false
)
@Singleton
public class ReqSosReport implements ApiCall
{
    private final StltSosReportApiCallHandler sosApiCallHandler;

    @Inject
    public ReqSosReport(StltSosReportApiCallHandler sosApiCallHandlerRef)
    {
        sosApiCallHandler = sosApiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgReqSosReport reqSosReport = MsgReqSosReport.parseDelimitedFrom(msgDataIn);
        sosApiCallHandler.handleSosReportRequest(new Date(reqSosReport.getSince()));
    }
}
