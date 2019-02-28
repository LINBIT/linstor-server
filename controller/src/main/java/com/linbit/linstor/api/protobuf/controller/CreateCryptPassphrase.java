package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.requests.MsgCrtCryptPassphraseOuterClass.MsgCrtCryptPassphrase;
import java.io.IOException;
import java.io.InputStream;

import javax.inject.Inject;
import javax.inject.Singleton;

@ProtobufApiCall(
    name = ApiConsts.API_CRT_CRYPT_PASS,
    description = "Enables encryption of the stored volume encryption keys"
)
@Singleton
public class CreateCryptPassphrase implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public CreateCryptPassphrase(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgCrtCryptPassphrase protoMsg = MsgCrtCryptPassphrase.parseDelimitedFrom(msgDataIn);
        apiCallAnswerer.answerApiCallRc(
            apiCallHandler.setMasterPassphrase(protoMsg.getPassphrase(), null)
        );
    }
}
