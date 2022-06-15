package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.apicallhandler.StltSosReportApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.responses.MsgReqSosCleanupOuterClass.MsgReqSosCleanup;

import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

import com.google.inject.Inject;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQ_SOS_REPORT_CLEANUP,
    description = "Deletes the temporary sos directory.",
    transactional = false
)
@Singleton
public class ReqSosReportCleanup implements ApiCall
{
    private final StltSosReportApiCallHandler sosApiCallHandler;
    private final Provider<Peer> peerProvider;
    private final Provider<Long> apiCallId;
    private final CtrlStltSerializer ctrlStltSerializer;

    @Inject
    public ReqSosReportCleanup(
        StltSosReportApiCallHandler sosApiCallHandlerRef,
        Provider<Peer> peerProviderRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
        CtrlStltSerializer ctrlStltSerializerRef
    )
    {
        sosApiCallHandler = sosApiCallHandlerRef;
        peerProvider = peerProviderRef;
        apiCallId = apiCallIdRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgReqSosCleanup reqSosReportCleanup = MsgReqSosCleanup.parseDelimitedFrom(msgDataIn);
        sosApiCallHandler.handleSosReportCleanup(
            reqSosReportCleanup.getSosReportName()
        );

        byte[] answer = ctrlStltSerializer
            .answerBuilder(InternalApiConsts.API_RSP_SOS_REPORT_CLEANUP_FINISHED, apiCallId.get())
            .build();

        peerProvider.get().sendMessage(answer, InternalApiConsts.API_RSP_SOS_REPORT_CLEANUP_FINISHED);
    }
}
