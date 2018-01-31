package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgDelCtrlCfgPropOuterClass.MsgDelCtrlCfgProp;
import com.linbit.linstor.security.AccessContext;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall
public class DeleteCtrlCfgProp extends BaseProtoApiCall
{
    private Controller controller;

    public DeleteCtrlCfgProp(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_DEL_CFG_VAL;
    }

    @Override
    public String getDescription()
    {
        return "Deletes a controller config property (if it exists)";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgDelCtrlCfgProp protoMsg = MsgDelCtrlCfgProp.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().deleteCtrlCfgProp(
            accCtx,
            protoMsg.getKey(),
            protoMsg.getNamespace()
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
