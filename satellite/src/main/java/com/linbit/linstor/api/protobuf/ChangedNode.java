package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_CHANGED_NODE,
    description = "Called by the controller to indicate that a node was modified"
)
@Singleton
public class ChangedNode implements ApiCallReactive
{
    private final DeviceManager deviceManager;
    private final ResponseSerializer responseSerializer;

    @Inject
    public ChangedNode(
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
        IntObjectId nodeId = IntObjectId.parseDelimitedFrom(msgDataIn);
        String nodeNameStr = nodeId.getName();
        UUID nodeUuid = ProtoUuidUtils.deserialize(nodeId.getUuid());

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
            .updateNode(
                nodeUuid,
                nodeName
            )
            .transform(responseSerializer::transform);
    }
}
