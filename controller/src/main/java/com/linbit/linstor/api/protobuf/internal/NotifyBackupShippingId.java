package com.linbit.linstor.api.protobuf.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntBackupShippingIdOuterClass.MsgIntBackupShippingId;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_ID,
    description = "Called by the satellite to notify the controller about a new updateId for a multipart upload",
    transactional = true
)
@Singleton
public class NotifyBackupShippingId implements ApiCallReactive
{
    private final BackupInfoManager backupInfoMgr;

    @Inject
    public NotifyBackupShippingId(
        BackupInfoManager backupInfoMgrRef
    )
    {
        backupInfoMgr = backupInfoMgrRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataInRef) throws IOException
    {
        MsgIntBackupShippingId ship = MsgIntBackupShippingId.parseDelimitedFrom(msgDataInRef);
        try
        {
            backupInfoMgr.abortCreateAddS3Entry(
                new NodeName(ship.getNodeName()),
                new ResourceName(ship.getRscName()),
                new SnapshotName(ship.getSnapName()),
                ship.getBackupName(),
                ship.getUploadId(),
                new RemoteName(ship.getRemoteName())
            );
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        return Flux.<byte[]>empty();
    }
}
