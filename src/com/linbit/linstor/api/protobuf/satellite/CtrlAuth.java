package com.linbit.linstor.api.protobuf.satellite;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntAuthOuterClass.MsgIntAuth;
import com.linbit.linstor.proto.javainternal.MsgIntExpectedFullSyncIdOuterClass.MsgIntExpectedFullSyncId;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class CtrlAuth extends BaseProtoApiCall
{
    private final Satellite satellite;

    public CtrlAuth(Satellite satelliteRef)
    {
        super(satelliteRef.getErrorReporter());
        satellite = satelliteRef;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_AUTH;
    }

    @Override
    public String getDescription()
    {
        return "Called by the controller to authenticate the controller to the satellite";
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer controllerPeer
    )
        throws IOException
    {
        // TODO: implement authentication
        MsgIntAuth auth = MsgIntAuth.parseDelimitedFrom(msgDataIn);
        String nodeName = auth.getNodeName();
        UUID nodeUuid = UUID.fromString(auth.getNodeUuid());
        UUID disklessStorPoolDfnUuid = UUID.fromString(auth.getNodeDisklessStorPoolDfnUuid());
        UUID disklessStorPoolUuid = UUID.fromString(auth.getNodeDisklessStorPoolUuid());


        ApiCallRcImpl apiCallRc = satellite.getApiCallHandler().authenticate(
            nodeUuid,
            nodeName,
            disklessStorPoolDfnUuid,
            disklessStorPoolUuid,
            controllerPeer
        );

        if (apiCallRc == null)
        {
            // all ok, send the new fullSyncId with the AUTH_ACCEPT msg
            controllerPeer.sendMessage(
                prepareMessage(
                    accCtx,
                    buildExpectedFullSyncIdMessage(satellite.getNextFullSyncId()),
                    controllerPeer,
                    msgId,
                    InternalApiConsts.API_AUTH_ACCEPT
                )
            );
        }
        else
        {
            // whatever happened should be in the apiCallRc
            controllerPeer.sendMessage(
                prepareMessage(
                    accCtx,
                    createApiCallResponse(accCtx, apiCallRc, controllerPeer),
                    controllerPeer,
                    msgId,
                    InternalApiConsts.API_AUTH_ERROR
                )
            );
        }
    }

    private byte[] buildExpectedFullSyncIdMessage(long nextFullSyncId) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MsgIntExpectedFullSyncId.newBuilder()
            .setExpectedFullSyncId(nextFullSyncId)
            .build()
            .writeDelimitedTo(baos);
        return baos.toByteArray();
    }
}
