package com.linbit.linstor.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntAuthSecretOuterClass.MsgIntAuthSecret;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class CtrlAuth extends BaseProtoApiCall
{
    private Satellite satellite;

    public CtrlAuth(Satellite satellite)
    {
        super(satellite.getErrorReporter());
        this.satellite = satellite;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_AUTH;
    }

    @Override
    public String getDescription()
    {
        return "The authentication api the controller has to call first";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        // TODO: implement authentication
        MsgIntAuthSecret secret = MsgIntAuthSecret.parseDelimitedFrom(msgDataIn);
        satellite.setControllerPeer(client);
    }

}
