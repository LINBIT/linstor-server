package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class IntAuthAccept extends BaseProtoApiCall
{
    private Controller controller;

    public IntAuthAccept(Controller controller)
    {
        super(controller.getErrorReporter());
        this.controller = controller;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_AUTH_ACCEPT;
    }

    @Override
    public String getDescription()
    {
        return "Satellite will call this api to confirm our authentication";
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        client.setAuthenticated(true);
        errorReporter.logDebug("Satellite '" + client.getNode().getName() + "' authenticated");

        controller.getApiCallHandler().sendFullSync(client);
    }
}
