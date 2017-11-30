package com.linbit.linstor.api.protobuf.controller.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import com.google.protobuf.ByteString;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.api.interfaces.serializer.CtrlAuthSerializer;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.proto.javainternal.MsgIntAuthOuterClass.MsgIntAuth;
import com.linbit.utils.UuidUtils;

public class AuthSerializerProto implements CtrlAuthSerializer
{
    @Override
    public byte[] getAuthMessage(Node node, byte[] sharedSecret) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MsgHeader.newBuilder()
            .setApiCall(InternalApiConsts.API_AUTH)
            .setMsgId(0)
            .build()
            .writeDelimitedTo(baos);
        MsgIntAuth.newBuilder()
            .setNodeUuid(ByteString.copyFrom(UuidUtils.asByteArray(node.getUuid())))
            .setNodeName(node.getName().displayValue)
            .setSharedSecret(ByteString.copyFrom(sharedSecret))
            .build()
            .writeDelimitedTo(baos);
        return baos.toByteArray();
    }

}
