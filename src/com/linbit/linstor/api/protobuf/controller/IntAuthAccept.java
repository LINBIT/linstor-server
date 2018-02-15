package com.linbit.linstor.api.protobuf.controller;

import com.google.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntExpectedFullSyncIdOuterClass.MsgIntExpectedFullSyncId;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_AUTH_ACCEPT,
    description = "Called by the satellite to indicate that controller authentication succeeded"
)
public class IntAuthAccept implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final CtrlApiCallHandler apiCallHandler;
    private final Peer client;

    @Inject
    public IntAuthAccept(
        ErrorReporter errorReporterRef,
        CtrlApiCallHandler apiCallHandlerRef,
        Peer clientRef
    )
    {
        errorReporter = errorReporterRef;
        apiCallHandler = apiCallHandlerRef;
        client = clientRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntExpectedFullSyncId msgIntExpectedFullSyncId = MsgIntExpectedFullSyncId.parseDelimitedFrom(msgDataIn);
        long expectedFullSyncId = msgIntExpectedFullSyncId.getExpectedFullSyncId();
        client.setAuthenticated(true);
        errorReporter.logDebug("Satellite '" + client.getNode().getName() + "' authenticated");

        apiCallHandler.sendFullSync(expectedFullSyncId);
    }
}
