package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntCryptKeyOuterClass.MsgIntCryptKey;

import java.io.IOException;
import java.io.InputStream;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.ByteString;

@ProtobufApiCall(
    name = InternalApiConsts.API_CRYPT_KEY,
    description = "Sets the crypt key"
)
@Singleton
public class CryptKey implements ApiCall
{

    private final StltApiCallHandler apiCallHandler;

    @Inject
    public CryptKey(StltApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgIntCryptKey protoMsg = MsgIntCryptKey.parseDelimitedFrom(msgDataIn);
        ByteString byteString = protoMsg.getCryptKey();
        byte[] key = new byte[byteString.size()];
        byteString.copyTo(key, 0);

        apiCallHandler.setCryptKey(key, protoMsg.getFullSyncId(), protoMsg.getUpdateId());
    }

}
