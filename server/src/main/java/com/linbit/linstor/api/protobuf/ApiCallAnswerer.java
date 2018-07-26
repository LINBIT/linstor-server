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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class ApiCallAnswerer
{
    private final ErrorReporter errorReporter;
    private final CommonSerializer commonSerializer;

    private final Peer peer;
    private final int msgId;

    @Inject
    public ApiCallAnswerer(
        ErrorReporter errorReporterRef,
        CommonSerializer commonSerializerRef,
        Peer peerRef,
        @Named(ApiModule.MSG_ID) int msgIdRef
    )
    {
        errorReporter = errorReporterRef;
        commonSerializer = commonSerializerRef;
        peer = peerRef;
        msgId = msgIdRef;
    }

    public void writeProtoMsgHeader(OutputStream out, String apiCallName)
        throws IOException
    {
        MsgHeader.Builder headerBld = MsgHeader.newBuilder();
        headerBld.setMsgId(msgId);
        headerBld.setApiCall(apiCallName);
        MsgHeader header = headerBld.build();
        header.writeDelimitedTo(out);
    }

    public void answerApiCallRc(ApiCallRc apiCallRc)
    {
        byte[] apiCallMsgData = commonSerializer.builder(ApiConsts.API_REPLY, msgId)
            .apiCallRcSeries(apiCallRc)
            .build();

        peer.sendMessage(apiCallMsgData);
    }

    public byte[] prepareMessage(byte[] protoMsgsBytes, String apicalltype)
    {
        MsgHeader protoHeader = MsgHeader.newBuilder()
            .setApiCall(apicalltype)
            .setMsgId(msgId)
            .build();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try
        {
            protoHeader.writeDelimitedTo(baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(
                ioExc,
                null,
                null,
                "IOException occured while generating protobuf api header"
            );
        }

        byte[] protoHeaderBytes = baos.toByteArray();
        byte[] apiCallData = new byte[protoHeaderBytes.length + protoMsgsBytes.length];

        // copy header data into apicalldata
        System.arraycopy(protoHeaderBytes, 0, apiCallData, 0, protoHeaderBytes.length);

        // copy proto message data into apicalldata if there is any
        if (protoMsgsBytes.length > 0)
        {
            System.arraycopy(protoMsgsBytes, 0, apiCallData, protoHeaderBytes.length, protoMsgsBytes.length);
        }

        return apiCallData;
    }
}
