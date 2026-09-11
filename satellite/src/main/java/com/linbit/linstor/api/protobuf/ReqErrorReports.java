package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.requests.MsgReqErrorReportOuterClass.MsgReqErrorReport;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.HashSet;

/**
 * Protobuf API call handler for requesting error reports.
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_REQ_ERROR_REPORTS,
    description = "Returns the requested error reports.",
    transactional = false
)
@Singleton
public class ReqErrorReports implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final Provider<Peer> peerProvider;

    @Inject
    public ReqErrorReports(StltApiCallHandler apiCallHandlerRef, Provider<Peer> peerProviderRef)
    {
        apiCallHandler = apiCallHandlerRef;
        peerProvider = peerProviderRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgReqErrorReport reqErrorReport = MsgReqErrorReport.parseDelimitedFrom(msgDataIn);
        @Nullable Instant since = reqErrorReport.hasSince() ? Instant.ofEpochMilli(reqErrorReport.getSince()) : null;
        @Nullable Instant to = reqErrorReport.hasTo() ? Instant.ofEpochMilli(reqErrorReport.getTo()) : null;

        peerProvider.get().sendMessage(
            apiCallHandler
                .listErrorReports(
                    new HashSet<>(reqErrorReport.getNodeNamesList()),
                    reqErrorReport.getWithContent(),
                    since,
                    to,
                    new HashSet<>(reqErrorReport.getIdsList()),
                    reqErrorReport.hasLimit() ? reqErrorReport.getLimit() : null,
                    reqErrorReport.hasOffset() ? reqErrorReport.getOffset() : null
                ),
            ApiConsts.API_LST_ERROR_REPORTS
        );
    }
}
