package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.api.pojo.StltRemotePojo;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.c2s.IntS3RemoteOuterClass.IntS3Remote;
import com.linbit.linstor.proto.javainternal.c2s.IntStltRemoteOuterClass.IntStltRemote;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyRemoteOuterClass.MsgIntApplyRemote;

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
public class ApplyRemote implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;

    @Inject
    public ApplyRemote(
        StltApiCallHandler apiCallHandlerRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntApplyRemote msgApplyRemote = MsgIntApplyRemote.parseDelimitedFrom(msgDataIn);

        if (msgApplyRemote.hasS3Remote())
        {
            S3RemotePojo s3remotePojo = asS3RemotePojo(
                msgApplyRemote.getS3Remote(),
                msgApplyRemote.getFullSyncId(),
                msgApplyRemote.getUpdateId()
            );
            apiCallHandler.applyS3RemoteChanges(s3remotePojo);
        }
        else if (msgApplyRemote.hasSatelliteRemote())
        {
            StltRemotePojo stltRemotePojo = asStltRemotePojo(
                msgApplyRemote.getSatelliteRemote(),
                msgApplyRemote.getFullSyncId(),
                msgApplyRemote.getUpdateId()
            );
            apiCallHandler.applyStltRemoteChanges(stltRemotePojo);
        }

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

    static StltRemotePojo asStltRemotePojo(IntStltRemote proto, long fullSyncId, long updateId)
    {
        return new StltRemotePojo(
            UUID.fromString(proto.getUuid()),
            proto.getName(),
            proto.getFlags(),
            proto.getTargetIp(),
            proto.getTargetPort(),
            proto.getUseZstd(),
            fullSyncId,
            updateId
        );
    }
}
