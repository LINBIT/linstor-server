package com.linbit.drbdmanage.api;

import com.google.protobuf.GeneratedMessageV3;
import com.linbit.ImplementationError;
import com.linbit.drbdmanage.ApiCall;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.ApiCallRc.RcEntry;
import com.linbit.drbdmanage.ApiConsts;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.drbdmanage.proto.MsgApiCallResponseOuterClass.MsgApiCallResponse;
import com.linbit.drbdmanage.proto.MsgApiCallResponseOuterClass.MsgApiCallResponse.Builder;
import com.linbit.drbdmanage.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.drbdmanage.security.AccessContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.event.Level;

/**
 * Base class for network APIs
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class BaseApiCall implements ApiCall
{
    protected final ErrorReporter errorReporter;

    public BaseApiCall(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
    }

    protected void writeMsgHeader(OutputStream out, int msgId, String apiCallName)
        throws IOException
    {
        MsgHeader.Builder headerBld = MsgHeader.newBuilder();
        headerBld.setMsgId(msgId);
        headerBld.setApiCall(apiCallName);
        MsgHeader header = headerBld.build();
        header.writeDelimitedTo(out);
    }

    protected void writeMsgContent(OutputStream out, GeneratedMessageV3 protobufsMsg)
        throws IOException
    {
        protobufsMsg.writeDelimitedTo(out);
    }

    @Override
    public void execute(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
    {
        try
        {
            executeImpl(accCtx, msg, msgId, msgDataIn, client);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(
                Level.ERROR,
                ioExc,
                accCtx,
                client,
                "IO error occured while executing the '" + getName() + "' API."
            );
        }
    }

    protected abstract void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException;

    protected void answerApiCallRc(
        AccessContext accCtx,
        Peer peer,
        int msgId,
        ApiCallRc apiCallRc
    )
    {
        Message apiCallAnswer = peer.createMessage();
        List<RcEntry> apiCallEntries = apiCallRc.getEntries();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (RcEntry apiCallEntry : apiCallEntries)
        {
            Builder msgApiCallResponseBuilder = MsgApiCallResponse.newBuilder();
            MsgApiCallResponse protoMsg;

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
            msgApiCallResponseBuilder.addAllObjRefs(asLinStorMapEntryList(apiCallEntry.getObjRefs()));
            msgApiCallResponseBuilder.addAllVariables(asLinStorMapEntryList(apiCallEntry.getVariables()));

            protoMsg = msgApiCallResponseBuilder.build();
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
            // cannot happen
            errorReporter.reportError(
                new ImplementationError(
                    "ByteArrayOutputStream.close() should never throw an IOException, but it did",
                    ioExc
                )
            );
        }

        MsgHeader protoHeader = MsgHeader.newBuilder()
            .setApiCall(ApiConsts.API_REPLY)
            .setMsgId(msgId)
            .build();
        baos = new ByteArrayOutputStream();
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
                "IOException occured while generating ApiCallResponse header"
            );
        }

        byte[] protoHeaderBytes = baos.toByteArray();
        byte[] apiCallData = new byte[protoHeaderBytes.length + protoMsgsBytes.length];
        System.arraycopy(protoHeaderBytes, 0, apiCallData, 0, protoHeaderBytes.length);
        System.arraycopy(protoMsgsBytes, 0, apiCallData, protoHeaderBytes.length, protoMsgsBytes.length);

        try
        {
            apiCallAnswer.setData(apiCallData);
        }
        catch (IllegalMessageStateException illegalMsgStateExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Setting the byte[] data for a new message failed",
                    illegalMsgStateExc
                )
            );
        }

        try
        {
            peer.sendMessage(apiCallAnswer);
        }
        catch (IllegalMessageStateException illegalMsgStateExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Sending a new message failed",
                    illegalMsgStateExc
                )
            );
        }
    }

    protected Map<String, String> asMap(List<LinStorMapEntry> list)
    {
        Map<String, String> map = new TreeMap<>();
        for (LinStorMapEntry entry : list)
        {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    protected List<LinStorMapEntry> asLinStorMapEntryList(Map<String, String> map)
    {
        List<LinStorMapEntry> entries = new ArrayList<>(map.size());
        for (Map.Entry<String, String> entry : map.entrySet())
        {
            entries.add(LinStorMapEntry.newBuilder()
                .setKey(entry.getKey())
                .setValue(entry.getValue())
                .build()
            );
        }
        return entries;
    }
}
