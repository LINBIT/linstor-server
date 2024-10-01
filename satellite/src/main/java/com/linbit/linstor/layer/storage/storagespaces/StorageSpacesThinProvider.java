package com.linbit.linstor.layer.storage.storagespaces;

import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.storagespaces.StorageSpacesData;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class StorageSpacesThinProvider extends StorageSpacesProvider
{
        /* Those are in bytes. */
        /* One thin volume uses that much bytes at least: */
    private final Long MINIMAL_THIN_SIZE_ON_DISK = 256L*1024L*1024L;
        /* One thin volume (the usable partition) can be that much bytes
           maximum: 4EiB - 17MiB (for the partition overhead): */
    private final Long MAXIMAL_THIN_SIZE = 1024L*1024L*1024L*1024L*1024L*1024L*4L - 17L*1024L*1024L;

    @Inject
    public StorageSpacesThinProvider(
        ErrorReporter errorReporter,
        ExtCmdFactoryStlt extCmdFactory,
        @DeviceManagerContext AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider,
        SnapshotShippingService snapShipMrgRef,
        StltExtToolsChecker extToolsCheckerRef,
        CloneService cloneServiceRef,
        BackupShippingMgr backupShipMgrRef,
        FileSystemWatch fileSystemWatchRef
    )
    {
        super(
            errorReporter,
            extCmdFactory,
            storDriverAccCtx,
            stltConfigAccessor,
            wipeHandler,
            notificationListenerProvider,
            transMgrProvider,
            "STORAGE_SPACES_THIN",
            DeviceProviderKind.STORAGE_SPACES_THIN,
            snapShipMrgRef,
            extToolsCheckerRef,
            cloneServiceRef,
            backupShipMgrRef,
            fileSystemWatchRef
        );
    }

    @Override
    protected void createLvImpl(StorageSpacesData<Resource> vlmData)
        throws StorageException, AccessDeniedException
    {
        super.createLvImpl(vlmData);
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPoolInfo storPoolRef) throws AccessDeniedException, StorageException
    {
        SpaceInfo info = super.getSpaceInfo(storPoolRef);

        if (info.freeCapacity*1024 >= MINIMAL_THIN_SIZE_ON_DISK)
        {
            info.freeCapacity = MAXIMAL_THIN_SIZE / 1024;
        }
        return info;
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.STORAGE_SPACES_THIN;
    }
}
