package com.linbit.linstor.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ChangedRsc extends BaseProtoApiCall
{
    private final Satellite satellite;

    public ChangedRsc(Satellite satellite)
    {
        super(satellite.getErrorReporter());
        this.satellite = satellite;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_CHANGED_RSC;
    }

    @Override
    public String getDescription()
    {
        return "Called by the controller to indicate that a resource was modified";
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
        String rscName = null;
        try
        {
            MsgIntObjectId rscId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
            rscName = rscId.getName();
            UUID rscUuid = UUID.fromString(rscId.getUuid());

            Map<NodeName, UUID> changedNodes = new TreeMap<>();
            // controller could notify us (in future) about changes in other nodes
            NodeData localNode = satellite.getLocalNode();
            changedNodes.put(
                localNode.getName(),
                rscUuid
            );
            satellite.getDeviceManager().getUpdateTracker().updateResource(
                new ResourceName(rscName),
                changedNodes
            );
        }
        catch (InvalidNameException invalidNameExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Controller sent an illegal resource name: " + rscName + ".",
                    invalidNameExc
                )
            );
        }
    }
}
