package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_CHANGED_STOR_POOL,
    description = "Called by the controller to indicate that a storage pool was modified"
)
@Singleton
public class ChangedStorPool implements ApiCallReactive
{
    private final DeviceManager deviceManager;
    private final ResponseSerializer responseSerializer;

    @Inject
    public ChangedStorPool(
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
        IntObjectId nodeNameId = IntObjectId.parseDelimitedFrom(msgDataIn);
        String nodeNameStr = nodeNameId.getName();
        IntObjectId storPoolId = IntObjectId.parseDelimitedFrom(msgDataIn);
        String storPoolNameStr = storPoolId.getName();
        UUID storPoolUuid = ProtoUuidUtils.deserialize(storPoolId.getUuid());

        NodeName nodeName;
        StorPoolName storPoolName;
        try
        {
            nodeName = new NodeName(nodeNameStr);
            storPoolName = new StorPoolName(storPoolNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(
                "Controller sent an invalid stor pool name",
                invalidNameExc
            );
        }

        return deviceManager.getUpdateTracker()
            .updateStorPool(
                storPoolUuid,
                nodeName,
                storPoolName
            )
            .transform(responseSerializer::transform);
    }
}
