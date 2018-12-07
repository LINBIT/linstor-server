package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

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
        MsgIntObjectId rscId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        MsgIntObjectId snapshotId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        String rscNameStr = rscId.getName();
        UUID snapshotUuid = UUID.fromString(snapshotId.getUuid());
        String snapshotNameStr = snapshotId.getName();

        ResourceName rscName;
        SnapshotName snapshotName;
        try
        {
            rscName = new ResourceName(rscNameStr);
            snapshotName = new SnapshotName(snapshotNameStr);
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
