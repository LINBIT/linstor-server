package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.requests.MsgDelErrorReportsOuterClass.MsgDelErrorReports;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_DEL_ERROR_REPORT,
    description = "Deletes error reports from the h2 database",
    transactional = false
)
@Singleton
public class DelErrorReports implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final Provider<Peer> peerProvider;

    @Inject
    public DelErrorReports(StltApiCallHandler apiCallHandlerRef, Provider<Peer> peerProviderRef)
    {
        apiCallHandler = apiCallHandlerRef;
        peerProvider = peerProviderRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgDelErrorReports reqErrorReport = MsgDelErrorReports.parseDelimitedFrom(msgDataIn);
        Date since = reqErrorReport.hasSince() ? new Date(reqErrorReport.getSince()) : null;
        Date to = reqErrorReport.hasTo() ? new Date(reqErrorReport.getTo()) : null;

        peerProvider.get().sendMessage(
            apiCallHandler.deleteErrorReports(
                since,
                to,
                reqErrorReport.hasException() ? reqErrorReport.getException() : null,
                reqErrorReport.hasVersion() ? reqErrorReport.getVersion() : null,
                reqErrorReport.getIdsList()
            ),
            ApiConsts.API_DEL_ERROR_REPORT
        );
    }
}
