package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgSetCtrlCfgPropOuterClass.MsgSetCtrlCfgProp;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_SET_CFG_VAL,
    description = "Sets a controller config property (possibly overriding old value)."
)
public class SetCtrlCfgProp implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public SetCtrlCfgProp(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgSetCtrlCfgProp protoMsg = MsgSetCtrlCfgProp.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = apiCallHandler.setCtrlCfgProp(
            protoMsg.getKey(),
            protoMsg.getNamespace(),
            protoMsg.getValue()
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
