package com.linbit.linstor.api.protobuf.satellite.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;

import com.google.protobuf.ByteString;
import com.linbit.GenericName;
import com.linbit.linstor.api.interfaces.serializer.StltRequestSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import com.linbit.utils.UuidUtils;

public class GenericRequestSerializerProto<TYPE extends GenericName> implements StltRequestSerializer<TYPE>
{
    private ErrorReporter errorReporter;
    private String apiCall;

    public GenericRequestSerializerProto(ErrorReporter errorReporter, String apiCall)
    {
        this.errorReporter = errorReporter;
        this.apiCall = apiCall;
    }

    @Override
    public byte[] getRequestMessage(int msgId, UUID uuid, TYPE name)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] toSend;
        try
        {
            MsgHeader.newBuilder()
                .setApiCall(apiCall)
                .setMsgId(msgId)
                .build()
                .writeDelimitedTo(baos);
            MsgIntObjectId.newBuilder()
                .setUuid(ByteString.copyFrom(UuidUtils.asByteArray(uuid)))
                .setName(name.displayValue)
                .build()
                .writeDelimitedTo(baos);
            toSend = baos.toByteArray();
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            toSend = new byte[0];
        }
        return toSend;
    }
}
