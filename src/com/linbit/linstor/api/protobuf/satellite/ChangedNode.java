package com.linbit.linstor.api.protobuf.satellite;

import com.google.inject.Inject;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_CHANGED_NODE,
    description = "Called by the controller to indicate that a node was modified"
)
public class ChangedNode implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final DeviceManager deviceManager;

    @Inject
    public ChangedNode(ErrorReporter errorReporterRef, DeviceManager deviceManagerRef)
    {
        errorReporter = errorReporterRef;
        deviceManager = deviceManagerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        String nodeName = null;
        try
        {
            MsgIntObjectId nodeId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
            nodeName = nodeId.getName();
            UUID nodeUuid = UUID.fromString(nodeId.getUuid());

            deviceManager.getUpdateTracker().updateNode(
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
