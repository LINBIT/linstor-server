package com.linbit.linstor.backupshipping;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

    public Set<AbsBackupShippingService> getServices(VlmProviderObject<Snapshot> snapVlmRef)
    {
        return getServices(((SnapshotVolume) snapVlmRef.getVolume()).getSnapshot());
    }

    public Set<AbsBackupShippingService> getServices(Snapshot snapshotRef)
    {
        return new HashSet<>(services.values());
    }

    public void allBackupPartsRegistered(Snapshot snapshotRef, String s3orStltRemoteName)
    {
        for (AbsBackupShippingService backupShippingService : services.values())
        {
            backupShippingService.allBackupPartsRegistered(snapshotRef, s3orStltRemoteName);
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

    public void removeSnapFromStartedShipments(String rscName, String snapName, String remoteName)
    {
        for (AbsBackupShippingService backupShippingService : services.values())
        {
            backupShippingService.removeSnapFromStartedShipments(rscName, snapName, remoteName);
        }
    }

    public Collection<AbsBackupShippingService> getAllServices()
    {
        return services.values();
    }
}
