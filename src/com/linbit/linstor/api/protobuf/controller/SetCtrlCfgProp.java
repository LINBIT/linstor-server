package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgSetCtrlCfgPropOuterClass.MsgSetCtrlCfgProp;
import com.linbit.linstor.security.AccessContext;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall
public class SetCtrlCfgProp extends BaseProtoApiCall
{
    private Controller controller;

    public SetCtrlCfgProp(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_SET_CFG_VAL;
    }

    @Override
    public String getDescription()
    {
        return "Sets a controller config property (possibly overriding old value).";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgSetCtrlCfgProp protoMsg = MsgSetCtrlCfgProp.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().setCtrlCfgProp(
            accCtx,
            protoMsg.getKey(),
            protoMsg.getNamespace(),
            protoMsg.getValue()
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
