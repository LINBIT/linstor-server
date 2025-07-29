package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtoUuidUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.SnapshotInternalCallHandler;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_IN_PROGRESS_SNAPSHOT,
    description = "Called by the satellite to request snapshot update data",
    transactional = false
)
@Singleton
public class IntRequestSnapshot implements ApiCall
{
    private final SnapshotInternalCallHandler snapshotInternalCallHandler;

    @Inject
    public IntRequestSnapshot(SnapshotInternalCallHandler apiCallHandlerRef)
    {
        snapshotInternalCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        IntObjectId resourceId = IntObjectId.parseDelimitedFrom(msgDataIn);
        IntObjectId snapshotId = IntObjectId.parseDelimitedFrom(msgDataIn);
        String resourceName = resourceId.getName();
        UUID snapshotUuid = ProtoUuidUtils.deserialize(snapshotId.getUuid());
        String snapshotName = snapshotId.getName();

        snapshotInternalCallHandler.handleSnapshotRequest(resourceName, snapshotUuid, snapshotName);
    }
}
