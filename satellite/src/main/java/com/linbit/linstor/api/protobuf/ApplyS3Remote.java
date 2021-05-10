package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.c2s.IntS3RemoteOuterClass.IntS3Remote;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyS3RemoteOuterClass.MsgIntApplyS3Remote;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_REMOTE,
    description = "Registeres a remote on this satellite"
)
@Singleton
public class ApplyS3Remote implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;

    @Inject
    public ApplyS3Remote(
        StltApiCallHandler apiCallHandlerRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntApplyS3Remote msgApplyS3Remote = MsgIntApplyS3Remote.parseDelimitedFrom(msgDataIn);

        S3RemotePojo s3remotePojo = asS3RemotePojo(
            msgApplyS3Remote.getS3Remote(),
            msgApplyS3Remote.getFullSyncId(),
            msgApplyS3Remote.getUpdateId()
        );
        apiCallHandler.applyS3RemoteChanges(s3remotePojo);
    }

    static S3RemotePojo asS3RemotePojo(IntS3Remote proto, long fullSyncId, long updateId)
    {
        return new S3RemotePojo(
            UUID.fromString(proto.getUuid()),
            proto.getName(),
            proto.getFlags(),
            proto.getEndpoint(),
            proto.getBucket(),
            proto.getRegion(),
            proto.getAccessKey().toByteArray(),
            proto.getSecretKey().toByteArray(),
            fullSyncId,
            updateId
        );
    }
}
