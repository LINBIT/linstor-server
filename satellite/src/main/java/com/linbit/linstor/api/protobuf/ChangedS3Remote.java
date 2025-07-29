package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_CHANGED_REMOTE,
    description = "Called by the controller to indicate that a s3-remote was modified"
)
@Singleton
public class ChangedS3Remote implements ApiCallReactive
{
    private final DeviceManager deviceManager;
    private final ResponseSerializer responseSerializer;

    @Inject
    public ChangedS3Remote(
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
        IntObjectId remoteId = IntObjectId.parseDelimitedFrom(msgDataIn);
        String remoteStr = remoteId.getName();
        UUID remoteUuid = ProtoUuidUtils.deserialize(remoteId.getUuid());

        RemoteName remoteName;
        try
        {
            remoteName = new RemoteName(remoteStr, true);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(
                "Controller sent an illegal remote name: " + remoteStr + ".",
                invalidNameExc
            );
        }

        return deviceManager.getUpdateTracker()
            .updateS3Remote(
                remoteUuid,
                remoteName
            )
            .transform(responseSerializer::transform);
    }
}
