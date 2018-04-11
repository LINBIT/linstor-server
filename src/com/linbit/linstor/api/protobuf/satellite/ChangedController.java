package com.linbit.linstor.api.protobuf.satellite;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;

@ProtobufApiCall(
    name = InternalApiConsts.API_CHANGED_CONTROLLER,
    description = "Called by the controller to indicate that a controller properties changed."
)
public class ChangedController implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final DeviceManager deviceManager;

    @Inject
    public ChangedController(ErrorReporter errorReporterRef, DeviceManager deviceManagerRef)
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

            deviceManager.getUpdateTracker().updateController(
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
