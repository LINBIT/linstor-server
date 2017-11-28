package com.linbit.linstor.api.protobuf.controller.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.google.protobuf.ByteString;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AuthSerializer;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.proto.javainternal.MsgIntAuthSecretOuterClass.MsgIntAuthSecret;

public class AuthSerializerProto implements AuthSerializer
{
    @Override
    public byte[] getAuthMessage(byte[] sharedSecret) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MsgHeader.newBuilder()
            .setApiCall(ApiConsts.API_AUTH)
            .setMsgId(0)
            .build()
            .writeDelimitedTo(baos);
        MsgIntAuthSecret.newBuilder()
            .setSharedSecret(ByteString.copyFrom(sharedSecret))
            .build()
            .writeDelimitedTo(baos);
        return baos.toByteArray();
    }

}
