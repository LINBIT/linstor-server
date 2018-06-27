package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgEnterCryptPassphraseOuterClass.MsgEnterCryptPassphrase;

import java.io.IOException;
import java.io.InputStream;

import javax.inject.Inject;

@ProtobufApiCall(
    name = ApiConsts.API_ENTER_CRYPT_PASS,
    description = "Used to enter the passphrase used for encryption of the stored volume encryption keys"
)
public class EnterCryptPassphrase implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public EnterCryptPassphrase(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgEnterCryptPassphrase protoMsg = MsgEnterCryptPassphrase.parseDelimitedFrom(msgDataIn);
        apiCallAnswerer.answerApiCallRc(
            apiCallHandler.enterPassphrase(protoMsg.getPassphrase())
        );
    }
}
