package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.requests.MsgReqSosReportOuterClass.MsgReqSosReport;

import javax.inject.Provider;
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
    private final StltApiCallHandler apiCallHandler;
    private final Provider<Peer> peerProvider;

    @Inject
    public ReqSosReport(StltApiCallHandler apiCallHandlerRef, Provider<Peer> peerProviderRefProvider)
    {
        apiCallHandler = apiCallHandlerRef;
        peerProvider = peerProviderRefProvider;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgReqSosReport reqSosReport = MsgReqSosReport.parseDelimitedFrom(msgDataIn);
        peerProvider.get().sendMessage(
            apiCallHandler.listSosReport(new Date(reqSosReport.getSince())),
            InternalApiConsts.API_RSP_SOS_REPORT
        );
    }
}
