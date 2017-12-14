package com.linbit.linstor.api.protobuf.controller.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import com.google.protobuf.ByteString;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.api.interfaces.serializer.CtrlAuthSerializer;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.proto.javainternal.MsgIntAuthOuterClass.MsgIntAuth;

public class AuthSerializerProto implements CtrlAuthSerializer
{
    @Override
    public byte[] getAuthMessage(Node satelliteNode, byte[] sharedSecret)
        throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MsgHeader.newBuilder()
            .setApiCall(InternalApiConsts.API_AUTH)
            .setMsgId(0)
            .build()
            .writeDelimitedTo(baos);
        MsgIntAuth.newBuilder()
            .setNodeUuid(satelliteNode.getUuid().toString())
            .setNodeName(satelliteNode.getName().displayValue)
            .setSharedSecret(ByteString.copyFrom(sharedSecret))
            .build()
            .writeDelimitedTo(baos);
        return baos.toByteArray();
    }

}
