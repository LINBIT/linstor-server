package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.io.OutputStream;

public class ApiCallAnswerer
{
    private final ErrorReporter errorReporter;
    private final CommonSerializer commonSerializer;

    private final Peer peer;
    private final long apiCallId;

    @Inject
    public ApiCallAnswerer(
        ErrorReporter errorReporterRef,
        CommonSerializer commonSerializerRef,
        Peer peerRef,
        @Named(ApiModule.API_CALL_ID) long apiCallIdRef
    )
    {
        errorReporter = errorReporterRef;
        commonSerializer = commonSerializerRef;
        peer = peerRef;
        apiCallId = apiCallIdRef;
    }

    public void writeAnswerHeader(OutputStream out, String apiCallName)
        throws IOException
    {
        MsgHeader header = MsgHeader.newBuilder()
            .setMsgType(MsgHeader.MsgType.ANSWER)
            .setApiCallId(apiCallId)
            .setMsgContent(apiCallName)
            .build();
        header.writeDelimitedTo(out);
    }

    public void answerApiCallRc(ApiCallRc apiCallRc)
    {
        byte[] apiCallMsgData = commonSerializer.answerBuilder(ApiConsts.API_REPLY, apiCallId)
            .apiCallRcSeries(apiCallRc)
            .build();

        peer.sendMessage(apiCallMsgData);
    }

    public byte[] prepareOnewayMessage(byte[] protoMsgsBytes, String apiCall)
    {
        return commonSerializer
            .onewayBuilder(apiCall)
            .bytes(protoMsgsBytes)
            .build();
    }
}
