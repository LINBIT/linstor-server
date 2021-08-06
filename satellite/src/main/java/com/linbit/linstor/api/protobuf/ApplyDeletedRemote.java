package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntRemoteDeletedOuterClass.MsgIntRemoteDeleted;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_DELETED_REMOTE,
    description = "Response to a requested but already deleted remote"
)
@Singleton
public class ApplyDeletedRemote implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;

    @Inject
    public ApplyDeletedRemote(
        StltApiCallHandler apiCallHandlerRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntRemoteDeleted msgApplyDeletedRemote = MsgIntRemoteDeleted
            .parseDelimitedFrom(msgDataIn);

        S3RemotePojo s3remotePojo = asS3RemotePojo(
            msgApplyDeletedRemote.getRemoteName(),
            msgApplyDeletedRemote.getFullSyncId(),
            msgApplyDeletedRemote.getUpdateId()
        );
        apiCallHandler.applyDeletedS3RemoteChanges(s3remotePojo);
    }

    static S3RemotePojo asS3RemotePojo(String s3remoteName, long fullSyncId, long updateId)
    {
        return new S3RemotePojo(
            null,
            s3remoteName,
            0,
            "",
            "",
            "",
            new byte[0],
            new byte[0],
            fullSyncId,
            updateId
        );
    }
}
