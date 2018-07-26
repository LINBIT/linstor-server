package com.linbit.linstor.api.protobuf.satellite;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgReqErrorReportOuterClass.MsgReqErrorReport;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_REQ_ERROR_REPORTS,
    description = "Returns the requested error reports."
)
public class ReqErrorReports implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final Peer client;

    @Inject
    public ReqErrorReports(StltApiCallHandler apiCallHandlerRef, Peer clientRef)
    {
        apiCallHandler = apiCallHandlerRef;
        client = clientRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgReqErrorReport reqErrorReport = MsgReqErrorReport.parseDelimitedFrom(msgDataIn);
        Optional<Date> since = Optional.ofNullable(
            reqErrorReport.hasSince() ? new Date(reqErrorReport.getSince()) : null);
        Optional<Date> to = Optional.ofNullable(reqErrorReport.hasTo() ? new Date(reqErrorReport.getTo()) : null);

        client.sendMessage(
            apiCallHandler
                .listErrorReports(
                    new HashSet<>(reqErrorReport.getNodeNamesList()),
                    reqErrorReport.getWithContent(),
                    since,
                    to,
                    new HashSet<>(reqErrorReport.getIdsList()))
        );
    }
}
