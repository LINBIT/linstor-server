package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass.MsgApiCallResponse;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass.MsgApiCallResponse.Builder;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class ApiCallAnswerer
{
    private final ErrorReporter errorReporter;

    private final AccessContext accCtx;
    private final Peer peer;
    private final int msgId;

    @Inject
    public ApiCallAnswerer(
        ErrorReporter errorReporterRef,
        @PeerContext AccessContext accCtxRef,
        Peer peerRef,
        @Named(ApiModule.MSG_ID) int msgIdRef
    )
    {
        errorReporter = errorReporterRef;
        accCtx = accCtxRef;
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
        byte[] apiCallMsgData = createApiCallResponse(apiCallRc);
        byte[] apiCallData = prepareMessage(apiCallMsgData, ApiConsts.API_REPLY);

        peer.sendMessage(apiCallData);
    }

    public byte[] createApiCallResponse(ApiCallRc apiCallRc)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (RcEntry apiCallEntry : apiCallRc.getEntries())
        {
            Builder msgApiCallResponseBuilder = MsgApiCallResponse.newBuilder();

            msgApiCallResponseBuilder.setRetCode(apiCallEntry.getReturnCode());
            if (apiCallEntry.getCauseFormat() != null)
            {
                msgApiCallResponseBuilder.setCauseFormat(apiCallEntry.getCauseFormat());
            }
            if (apiCallEntry.getCorrectionFormat() != null)
            {
                msgApiCallResponseBuilder.setCorrectionFormat(apiCallEntry.getCorrectionFormat());
            }
            if (apiCallEntry.getDetailsFormat() != null)
            {
                msgApiCallResponseBuilder.setDetailsFormat(apiCallEntry.getDetailsFormat());
            }
            if (apiCallEntry.getMessageFormat() != null)
            {
                msgApiCallResponseBuilder.setMessageFormat(apiCallEntry.getMessageFormat());
            }
            msgApiCallResponseBuilder.addAllObjRefs(ProtoMapUtils.fromMap(apiCallEntry.getObjRefs()));
            msgApiCallResponseBuilder.addAllVariables(ProtoMapUtils.fromMap(apiCallEntry.getVariables()));

            MsgApiCallResponse protoMsg = msgApiCallResponseBuilder.build();

            try
            {
                protoMsg.writeDelimitedTo(baos);
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(
                    ioExc,
                    accCtx,
                    peer,
                    "IOException occured while generating ApiCallResponse"
                );
            }
        }

        byte[] protoMsgsBytes = baos.toByteArray();
        try
        {
            baos.close();
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "ByteArrayOutputStream.close() should never throw an IOException, but it did",
                    ioExc
                )
            );
        }
        return protoMsgsBytes;
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
                accCtx,
                peer,
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
