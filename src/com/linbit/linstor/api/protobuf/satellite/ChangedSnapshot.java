package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_CHANGED_IN_PROGRESS_SNAPSHOT,
    description = "Called by the controller to indicate that a snapshot was modified"
)
public class ChangedSnapshot implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final DeviceManager deviceManager;
    private final ControllerPeerConnector controllerPeerConnector;

    @Inject
    public ChangedSnapshot(
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
        try
        {
            MsgIntObjectId rscId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
            MsgIntObjectId snapshotId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
            String rscName = rscId.getName();
            UUID snapshotUuid = UUID.fromString(snapshotId.getUuid());
            String snapshotName = snapshotId.getName();

            deviceManager.getUpdateTracker().updateSnapshot(
                snapshotUuid,
                new ResourceName(rscName),
                new SnapshotName(snapshotName)
            );
        }
        catch (InvalidNameException invalidNameExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Controller sent an illegal resource/snapshot name: " + invalidNameExc.invalidName + ".",
                    invalidNameExc
                )
            );
        }
    }
}
