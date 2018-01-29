package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntExpectedFullSyncIdOuterClass.MsgIntExpectedFullSyncId;
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
        return "Called by the satellite to indicate that controller authentication succeeded";
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
        MsgIntExpectedFullSyncId msgIntExpectedFullSyncId = MsgIntExpectedFullSyncId.parseDelimitedFrom(msgDataIn);
        long expectedFullSyncId = msgIntExpectedFullSyncId.getExpectedFullSyncId();
        client.setAuthenticated(true);
        errorReporter.logDebug("Satellite '" + client.getNode().getName() + "' authenticated");

        controller.getApiCallHandler().sendFullSync(client, expectedFullSyncId);
    }
}
