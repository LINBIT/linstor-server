package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_IN_PROGRESS_SNAPSHOT,
    description = "Called by the satellite to request snapshot update data"
)
public class IntRequestSnapshot implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;

    @Inject
    public IntRequestSnapshot(CtrlApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntObjectId resourceId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        MsgIntObjectId snapshotId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        String resourceName = resourceId.getName();
        UUID snapshotUuid = UUID.fromString(snapshotId.getUuid());
        String snapshotName = snapshotId.getName();

        apiCallHandler.handleSnapshotRequest(resourceName, snapshotUuid, snapshotName);
    }
}
