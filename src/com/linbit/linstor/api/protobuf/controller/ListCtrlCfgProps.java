package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgReqCtrlCfgPropsOuterClass.MsgReqCtrlCfgProps;
import com.linbit.linstor.security.AccessContext;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall
public class ListCtrlCfgProps extends BaseProtoApiCall
{
    private Controller controller;

    public ListCtrlCfgProps(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_LST_CFG_VAL;
    }

    @Override
    public String getDescription()
    {
        return "Lists controller config properties";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgReqCtrlCfgProps protoMsg = MsgReqCtrlCfgProps.parseDelimitedFrom(msgDataIn);
        byte[] data = controller.getApiCallHandler().listCtrlCfg(
            accCtx,
            protoMsg.getKey(),
            protoMsg.getNamespace(),
            msgId
        );
        sendAnswer(client, data);
    }

}
