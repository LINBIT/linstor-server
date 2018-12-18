package com.linbit.linstor.storage.layer.provider.zfs;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.WipeHandler;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.ZfsCommands;
import com.linbit.utils.RemoveAfterDevMgrRework;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.SQLException;

@Singleton
public class ZfsThinProvider extends ZfsProvider
{
    @Inject
    public ZfsThinProvider(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        @DeviceManagerContext AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider
    )
    {
        super(
            errorReporter,
            extCmdFactory,
            storDriverAccCtx,
            stltConfigAccessor,
            wipeHandler,
            notificationListenerProvider,
            "ZFS-Thin"
        );
    }

    @Override
    protected void createLvImpl(Volume vlm) throws StorageException, AccessDeniedException, SQLException
    {
        long volumeSize = vlm.getUsableSize(storDriverAccCtx);
        if (volumeSize % DEFAULT_ZFS_EXTENT_SIZE != 0)
        {
            long origSize = volumeSize;
            volumeSize = ((volumeSize / DEFAULT_ZFS_EXTENT_SIZE) + 1) * DEFAULT_ZFS_EXTENT_SIZE;
            errorReporter.logInfo(
                String.format(
                    "Aligning size from %d KiB to %d KiB to be a multiple of extent size %d KiB",
                    origSize,
                    volumeSize,
                    DEFAULT_ZFS_EXTENT_SIZE
                )
            );
            vlm.setAllocatedSize(storDriverAccCtx, volumeSize);
        }
        ZfsCommands.create(
            extCmdFactory.create(),
            ((ZfsLayerDataStlt) vlm.getLayerData(storDriverAccCtx)).zpool,
            asLvIdentifier(vlm),
            volumeSize,
            true
        );
    }

    @Override
    protected String getZPool(StorPool storPool) throws AccessDeniedException
    {
        String zPool;
        try
        {
            zPool = DeviceLayerUtils.getNamespaceStorDriver(
                storPool.getProps(storDriverAccCtx)
            ).getProp(StorageConstants.CONFIG_ZFS_THIN_POOL_KEY);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return zPool;
    }

    @RemoveAfterDevMgrRework // this method should stay protected. Here it is made public
    // only to be accessible from LayeredSnapshotHelper
    @Override
    public String asLvIdentifier(VolumeDefinition vlmDfn)
    {
        return super.asLvIdentifier(vlmDfn);
    }
}
