package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_CHANGED_CONTROLLER,
    description = "Called by the controller to indicate that a controller properties changed."
)
public class ChangedController implements ApiCallReactive
{
    private final DeviceManager deviceManager;
    private final ResponseSerializer responseSerializer;

    @Inject
    public ChangedController(
        DeviceManager deviceManagerRef,
        ResponseSerializer responseSerializerRef
    )
    {
        deviceManager = deviceManagerRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgIntObjectId nodeId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        String nodeNameStr = nodeId.getName();
        UUID nodeUuid = UUID.fromString(nodeId.getUuid());

        NodeName nodeName;
        try
        {
            nodeName = new NodeName(nodeNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(
                "Controller sent an illegal node name: " + nodeNameStr + ".",
                invalidNameExc
            );
        }

        return deviceManager.getUpdateTracker()
            .updateController(
                nodeUuid,
                nodeName
            )
            .transform(responseSerializer::transform);
    }
}
