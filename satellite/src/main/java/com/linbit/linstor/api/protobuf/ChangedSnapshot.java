package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_CHANGED_IN_PROGRESS_SNAPSHOT,
    description = "Called by the controller to indicate that a snapshot was modified"
)
@Singleton
public class ChangedSnapshot implements ApiCallReactive
{
    private final DeviceManager deviceManager;
    private final ResponseSerializer responseSerializer;

    @Inject
    public ChangedSnapshot(
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
        IntObjectId rscId = IntObjectId.parseDelimitedFrom(msgDataIn);
        IntObjectId snapshotId = IntObjectId.parseDelimitedFrom(msgDataIn);
        String rscNameStr = rscId.getName();
        UUID snapshotUuid = UUID.fromString(snapshotId.getUuid());
        String snapshotNameStr = snapshotId.getName();

        ResourceName rscName;
        SnapshotName snapshotName;
        try
        {
            rscName = new ResourceName(rscNameStr);
            snapshotName = new SnapshotName(snapshotNameStr, true);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(
                "Controller sent an illegal resource/snapshot name: " + invalidNameExc.invalidName + ".",
                invalidNameExc
            );
        }

        return deviceManager.getUpdateTracker()
            .updateSnapshot(
                snapshotUuid,
                rscName,
                snapshotName
            )
            .transform(responseSerializer::transform);
    }
}
