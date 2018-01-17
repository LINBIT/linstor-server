package com.linbit.linstor.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ChangedNode extends BaseProtoApiCall
{
    private final Satellite satellite;

    public ChangedNode(Satellite satelliteRef)
    {
        super(satelliteRef.getErrorReporter());
        satellite = satelliteRef;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_CHANGED_NODE;
    }

    @Override
    public String getDescription()
    {
        return "Called by the controller to indicate that a node was modified";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        String nodeName = null;
        try
        {
            MsgIntObjectId nodeId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
            nodeName = nodeId.getName();
            UUID nodeUuid = UUID.fromString(nodeId.getUuid());

            satellite.getDeviceManager().getUpdateTracker().updateNode(
                nodeUuid,
                new NodeName(nodeName)
            );
        }
        catch (InvalidNameException invalidNameExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Controller sent an illegal node name: " + nodeName + ".",
                    invalidNameExc
                )
            );
        }
    }


}
