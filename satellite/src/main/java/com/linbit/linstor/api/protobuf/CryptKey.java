package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntCryptKeyOuterClass.MsgIntCryptKey;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

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

        apiCallHandler.setCryptKey(
            protoMsg.getCryptKey().toByteArray(),
            protoMsg.getCryptHash().toByteArray(),
            protoMsg.getCryptSalt().toByteArray(),
            protoMsg.getEncCryptKey().toByteArray(),
            protoMsg.getFullSyncId(),
            protoMsg.getUpdateId()
        );
    }

}
