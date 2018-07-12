package com.linbit.linstor.api.protobuf.satellite;

import javax.inject.Inject;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_CHANGED_RSC,
    description = "Called by the controller to indicate that a resource was modified"
)
public class ChangedRsc implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final DeviceManager deviceManager;
    private final ControllerPeerConnector controllerPeerConnector;

    @Inject
    public ChangedRsc(
        ErrorReporter errorReporterRef,
        DeviceManager deviceManagerRef,
        ControllerPeerConnector controllerPeerConnectorRef
    )
    {
        errorReporter = errorReporterRef;
        deviceManager = deviceManagerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
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
            changedNodes.put(
                controllerPeerConnector.getLocalNodeName(),
                rscUuid
            );
            deviceManager.getUpdateTracker().updateResource(
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
