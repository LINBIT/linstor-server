package com.linbit.linstor.api.protobuf.satellite;

import com.google.inject.Inject;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.StorPoolName;
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
    name = InternalApiConsts.API_CHANGED_STOR_POOL,
    description = "Called by the controller to indicate that a storage pool was modified"
)
public class ChangedStorPool implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final DeviceManager deviceManager;
    private final ControllerPeerConnector controllerPeerConnector;

    @Inject
    public ChangedStorPool(
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
            MsgIntObjectId storPoolId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
            String storPoolName = storPoolId.getName();
            UUID storPoolUuid = UUID.fromString(storPoolId.getUuid());

            Map<NodeName, UUID> nodesUpd = new TreeMap<>();
            nodesUpd.put(controllerPeerConnector.getLocalNode().getName(), storPoolUuid);
            deviceManager.getUpdateTracker().updateStorPool(
                storPoolUuid,
                new StorPoolName(storPoolName)
            );
        }
        catch (InvalidNameException invalidNameExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Controller sent an invalid stor pool name",
                    invalidNameExc
                )
            );
        }
    }

}
