package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntPrimaryOuterClass.MsgIntPrimary;
import com.linbit.linstor.security.AccessContext;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall
public class PrimaryResource extends BaseProtoApiCall
{
    private final Satellite satellite;

    public PrimaryResource(Satellite satelliteRef)
    {
        super(satelliteRef.getErrorReporter());
        satellite = satelliteRef;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_PRIMARY_RSC;
    }

    @Override
    public String getDescription()
    {
        return "Controller notifies the satellite that one of his resources should become primary";
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
        MsgIntPrimary msgReqPrimary = MsgIntPrimary.parseDelimitedFrom(msgDataIn);
        satellite.getApiCallHandler().handlePrimaryResource(
            controllerPeer,
            msgId,
            msgReqPrimary.getRscName(),
            UUID.fromString(msgReqPrimary.getRscUuid())
        );
    }
}
