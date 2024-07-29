package com.linbit.linstor.backupshipping;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class BackupShippingMgr
{
    private final AccessContext accCtx;
    private final RemoteMap remoteMap;
    private final Map<RemoteType, AbsBackupShippingService> services;

    @Inject
    public BackupShippingMgr(
        @SystemContext AccessContext accCtxRef,
        RemoteMap remoteMapRef,
        BackupShippingS3Service backupShippingS3Ref,
        BackupShippingL2LService backupShippingL2LRef
    )
    {
        accCtx = accCtxRef;
        remoteMap = remoteMapRef;
        services = new HashMap<>();

        services.put(RemoteType.S3, backupShippingS3Ref);
        services.put(RemoteType.SATELLITE, backupShippingL2LRef);
    }

    public @Nullable AbsBackupShippingService getService(RemoteType remoteType)
    {
        return services.get(remoteType);
    }

    public @Nullable AbsBackupShippingService getService(AbsRemote remote)
    {
        return getService(remote.getType());
    }

    public @Nullable AbsBackupShippingService getService(VlmProviderObject<Snapshot> snapVlmRef)
    {
        return getService(((SnapshotVolume) snapVlmRef.getVolume()).getSnapshot());
    }

    public @Nullable AbsBackupShippingService getService(Snapshot snapshotRef)
    {
        AbsBackupShippingService service = null;
        try
        {
            String key = null;
            if (snapshotRef.getFlags().isSet(accCtx, Snapshot.Flags.BACKUP_SOURCE))
            {
                // if the snapshot is the source, the remote is the target
                key = InternalApiConsts.KEY_BACKUP_TARGET_REMOTE;
            }
            else if (snapshotRef.getFlags().isSet(accCtx, Snapshot.Flags.BACKUP_TARGET))
            {
                key = InternalApiConsts.KEY_BACKUP_SRC_REMOTE;
            }

            if (key != null)
            {
                String remoteNameStr = snapshotRef.getSnapProps(accCtx)
                    .getProp(
                        key,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );

                if (remoteNameStr != null)
                {
                    AbsRemote remote = remoteMap.get(new RemoteName(remoteNameStr, true));
                    if (remote == null)
                    {
                        throw new ImplementationError(
                            "Remote " + remoteNameStr + " must not be null if the property is set"
                        );
                    }

                    service = getService(remote);
                }
            }
        }
        catch (InvalidKeyException | InvalidNameException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return service;
    }

    public void allBackupPartsRegistered(Snapshot snapshotRef)
    {
        AbsBackupShippingService backupShippingService = getService(snapshotRef);
        if (backupShippingService != null)
        {
            backupShippingService.allBackupPartsRegistered(snapshotRef);
        }
    }

    public void snapshotDeleted(Snapshot snapshotRef)
    {
        /*
         * Try to delete snapshot from every backupShippingService, as the remoteKey identifying the "correct" service
         * might already have been deleted
         */
        for (AbsBackupShippingService backupShippingService : services.values())
        {
            backupShippingService.snapshotDeleted(snapshotRef);
        }
    }

    public void killAllShipping() throws StorageException
    {
        for (AbsBackupShippingService backupShippingService : services.values())
        {
            backupShippingService.killAllShipping(false);
        }
    }

    public void removeSnapFromStartedShipments(String rscName, String snapName)
    {
        for (AbsBackupShippingService backupShippingService : services.values())
        {
            backupShippingService.removeSnapFromStartedShipments(rscName, snapName);
        }
    }

    public Collection<AbsBackupShippingService> getAllServices()
    {
        return services.values();
    }
}
