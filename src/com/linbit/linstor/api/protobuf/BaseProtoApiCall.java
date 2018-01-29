package com.linbit.linstor.api.protobuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.BaseApiCall;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass.MsgApiCallResponse;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass.MsgApiCallResponse.Builder;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.security.AccessContext;
import com.linbit.utils.UuidUtils;

public abstract class BaseProtoApiCall extends BaseApiCall
{
    public BaseProtoApiCall(ErrorReporter errorReporterRef)
    {
        super(errorReporterRef);
    }

    protected void writeProtoMsgHeader(OutputStream out, int msgId, String apiCallName)
        throws IOException
    {
        MsgHeader.Builder headerBld = MsgHeader.newBuilder();
        headerBld.setMsgId(msgId);
        headerBld.setApiCall(apiCallName);
        MsgHeader header = headerBld.build();
        header.writeDelimitedTo(out);
    }

    protected void writeProtoMsgContent(OutputStream out, GeneratedMessageV3 protobufsMsg)
        throws IOException
    {
        protobufsMsg.writeDelimitedTo(out);
    }

    public static Map<String, String> asMap(List<LinStorMapEntry> list)
    {
        Map<String, String> map = new TreeMap<>();
        for (LinStorMapEntry entry : list)
        {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    public static List<LinStorMapEntry> fromMap(Map<String, String> map)
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

    public static UUID asUuid(ByteString uuid)
    {
        return UuidUtils.asUuid(uuid.toByteArray());
    }

    public static ByteString fromUuid(UUID uuid)
    {
        return ByteString.copyFrom(UuidUtils.asByteArray(uuid));
    }

    @Override
    protected byte[] createApiCallResponse(
        AccessContext accCtx,
        ApiCallRc apiCallRc,
        Peer peer
    )
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (RcEntry apiCallEntry : apiCallRc.getEntries())
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
            msgApiCallResponseBuilder.addAllObjRefs(fromMap(apiCallEntry.getObjRefs()));
            msgApiCallResponseBuilder.addAllVariables(fromMap(apiCallEntry.getVariables()));

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
            errorReporter.reportError(
                new ImplementationError(
                    "ByteArrayOutputStream.close() should never throw an IOException, but it did",
                    ioExc
                )
            );
        }
        return protoMsgsBytes;
    }

    @Override
    protected byte[] prepareMessage(
        AccessContext accCtx,
        byte[] protoMsgsBytes,
        Peer peer,
        int msgId,
        String apicalltype
    )
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
        if(protoMsgsBytes.length > 0)
        {
            System.arraycopy(protoMsgsBytes, 0, apiCallData, protoHeaderBytes.length, protoMsgsBytes.length);
        }

        return apiCallData;
    }
}
