package com.linbit.linstor.api.protobuf.satellite.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;

import com.google.protobuf.ByteString;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.api.interfaces.serializer.StltResourceRequestSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import com.linbit.utils.UuidUtils;

public class ResourceRequestSerializerProto implements StltResourceRequestSerializer
{
    private ErrorReporter errorReporter;

    public ResourceRequestSerializerProto(ErrorReporter errorReporter)
    {
        this.errorReporter = errorReporter;
    }

    @Override
    public byte[] getRequestMessage(int msgId, UUID uuid, NodeName nodeName, ResourceName rscName)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] toSend;
        try
        {
            MsgHeader.newBuilder()
                .setMsgId(msgId)
                .setApiCall(InternalApiConsts.API_REQUEST_RSC)
                .build()
                .writeDelimitedTo(baos);
            MsgIntObjectId.newBuilder()
                // no nodeUuid
                .setName(nodeName.displayValue)
                .build()
                .writeDelimitedTo(baos);
            MsgIntObjectId.newBuilder()
                .setUuid(ByteString.copyFrom(UuidUtils.asByteArray(uuid)))
                .setName(rscName.displayValue)
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
