package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Optional;
import java.util.TreeSet;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlErrorListApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgReqErrorReportOuterClass.MsgReqErrorReport;
import reactor.core.publisher.Flux;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_REQ_ERROR_REPORTS,
    description = "Returns the requested error reports."
)
public class ReqErrorReports implements ApiCallReactive
{
    private final CtrlErrorListApiCallHandler errorListApiCallHandler;
    private final Peer client;

    @Inject
    public ReqErrorReports(CtrlErrorListApiCallHandler apiCallHandlerRef, Peer clientRef)
    {
        errorListApiCallHandler = apiCallHandlerRef;
        client = clientRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgReqErrorReport reqErrorReport = MsgReqErrorReport.parseDelimitedFrom(msgDataIn);
        Optional<Date> since = Optional.ofNullable(
            reqErrorReport.hasSince() ? new Date(reqErrorReport.getSince()) : null);
        Optional<Date> to = Optional.ofNullable(reqErrorReport.hasTo() ? new Date(reqErrorReport.getTo()) : null);
        return errorListApiCallHandler
            .listErrorReports(
                new TreeSet<>(reqErrorReport.getNodeNamesList()),
                reqErrorReport.getWithContent(),
                since,
                to,
                new TreeSet<>(reqErrorReport.getIdsList())
            );
    }
}
