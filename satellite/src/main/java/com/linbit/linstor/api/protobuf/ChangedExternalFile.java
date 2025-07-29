package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_CHANGED_EXTERNAL_FILE,
    description = "Called by the controller to indicate that an external file was modified"
)
@Singleton
public class ChangedExternalFile implements ApiCallReactive
{
    private final DeviceManager deviceManager;
    private final ResponseSerializer responseSerializer;

    @Inject
    public ChangedExternalFile(
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
        String extFileStr = nodeId.getName();
        UUID extFileUuid = ProtoUuidUtils.deserialize(nodeId.getUuid());

        ExternalFileName extFileName;
        try
        {
            extFileName = new ExternalFileName(extFileStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(
                "Controller sent an illegal external file name: " + extFileStr + ".",
                invalidNameExc
            );
        }

        return deviceManager.getUpdateTracker()
            .updateExternalFile(
                extFileUuid,
                extFileName
            )
            .transform(responseSerializer::transform);
    }
}
