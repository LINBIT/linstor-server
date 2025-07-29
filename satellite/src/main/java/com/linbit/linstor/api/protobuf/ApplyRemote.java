package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.EbsRemotePojo;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.api.pojo.StltRemotePojo;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.javainternal.c2s.IntEbsRemoteOuterClass.IntEbsRemote;
import com.linbit.linstor.proto.javainternal.c2s.IntS3RemoteOuterClass.IntS3Remote;
import com.linbit.linstor.proto.javainternal.c2s.IntStltRemoteOuterClass.IntStltRemote;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyRemoteOuterClass.MsgIntApplyRemote;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.TreeMap;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_REMOTE,
    description = "Registeres a remote on this satellite"
)
@Singleton
public class ApplyRemote implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final ErrorReporter errorReporter;

    @Inject
    public ApplyRemote(
        StltApiCallHandler apiCallHandlerRef,
        ErrorReporter errorReporterRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        errorReporter = errorReporterRef;
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
        else if (msgApplyRemote.hasEbsRemote())
        {
            EbsRemotePojo ebsRemotePojo = asEbsRemotePojo(
                msgApplyRemote.getEbsRemote(),
                msgApplyRemote.getFullSyncId(),
                msgApplyRemote.getUpdateId()
            );
            apiCallHandler.applyEbsRemoteChanges(ebsRemotePojo);
        }
        else
        {
            errorReporter.reportError(new ImplementationError("Message does not have a known remote type."));
        }

    }

    static S3RemotePojo asS3RemotePojo(IntS3Remote proto, long fullSyncId, long updateId)
    {
        return new S3RemotePojo(
            ProtoUuidUtils.deserialize(proto.getUuid()),
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

    static EbsRemotePojo asEbsRemotePojo(IntEbsRemote proto, long fullSyncId, long updateId)
    {
        return new EbsRemotePojo(
            ProtoUuidUtils.deserialize(proto.getUuid()),
            proto.getName(),
            proto.getFlags(),
            proto.getUrl(),
            proto.getAvailabilityZone(),
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
            ProtoUuidUtils.deserialize(proto.getUuid()),
            proto.getName(),
            proto.getFlags(),
            proto.getTargetIp(),
            new TreeMap<>(proto.getTargetPortsMap()),
            proto.getUseZstd(),
            fullSyncId,
            updateId
        );
    }
}
