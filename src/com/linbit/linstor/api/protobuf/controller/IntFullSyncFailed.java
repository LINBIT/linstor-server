package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall
public class IntFullSyncFailed extends BaseProtoApiCall
{
    public IntFullSyncFailed(Controller controller)
    {
        super(controller.getErrorReporter());
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_FULL_SYNC_FAILED;
    }

    @Override
    public String getDescription()
    {
        return "Satellite failed to apply our full sync";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer satellite)
        throws IOException
    {
        satellite.fullSyncFailed();
    }

}
