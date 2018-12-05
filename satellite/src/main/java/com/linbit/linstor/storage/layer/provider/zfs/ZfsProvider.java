package com.linbit.linstor.storage.layer.provider.zfs;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.AbsStorageProvider;
import com.linbit.linstor.storage.layer.provider.WipeHandler;
import com.linbit.linstor.storage.layer.provider.utils.ProviderUtils;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.ZfsCommands;
import com.linbit.linstor.storage.utils.ZfsUtils;
import com.linbit.linstor.storage.utils.ZfsUtils.ZfsInfo;
import com.linbit.linstor.storage2.layer.data.ZfsLayerData;
import com.linbit.linstor.storage2.layer.data.categories.VlmLayerData.Size;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@Singleton
public class ZfsProvider extends AbsStorageProvider<ZfsInfo, ZfsLayerDataStlt>
{
    // FIXME: FORMAT should be private, only made public for LayeredSnapshotHelper
    public static final String FORMAT_RSC_TO_ZFS_ID = "%s_%05d";
    private static final String FORMAT_ZFS_DEV_PATH = "/dev/%s/%s";
    private static final int TOLERANCE_FACTOR = 3;

    protected ZfsProvider(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        String subTypeDescr
    )
    {
        super(
            errorReporter,
            extCmdFactory,
            storDriverAccCtx,
            stltConfigAccessor,
            wipeHandler,
            notificationListenerProvider,
            subTypeDescr
        );
    }

    @Inject
    public ZfsProvider(
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
            "ZFS"
        );
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        return ZfsUtils.getZPoolFreeSize(extCmdFactory.create(), changedStoragePoolStrings);
    }

    @Override
    protected Map<String, ZfsInfo> getInfoListImpl(Collection<Volume> volumes) throws StorageException
    {
        return ZfsUtils.getZfsList(extCmdFactory.create());
    }

    @Override
    protected String asLvIdentifier(Volume vlm)
    {
        return asLvIdentifier(vlm.getVolumeDefinition());
    }

    @Override
    protected String asLvIdentifier(ResourceName resourceName, VolumeNumber volumeNumber)
    {
        return String.format(
            FORMAT_RSC_TO_ZFS_ID,
            resourceName.displayValue,
            volumeNumber.value
        );
    }

    private String asFullQualifiedLvIdentifier(SnapshotVolume snapVlm)
        throws AccessDeniedException, SQLException
    {
        return getStorageName(snapVlm) + File.separator + asLvIdentifier(snapVlm);
    }

    private String asLvIdentifier(SnapshotVolume snapVlm)
    {
        return
            asLvIdentifier(snapVlm.getSnapshotVolumeDefinition()) + "@" +
            snapVlm.getSnapshotName().displayValue;
    }


    @Override
    protected void createLvImpl(Volume vlm) throws StorageException, AccessDeniedException, SQLException
    {
        ZfsCommands.create(
            extCmdFactory.create(),
            ((ZfsLayerDataStlt) vlm.getLayerData(storDriverAccCtx)).zpool,
            asLvIdentifier(vlm),
            vlm.getVolumeDefinition().getVolumeSize(storDriverAccCtx),
            false
        );
    }

    @Override
    protected void resizeLvImpl(Volume vlm) throws StorageException, AccessDeniedException, SQLException
    {
        ZfsCommands.resize(
            extCmdFactory.create(),
            ((ZfsLayerDataStlt) vlm.getLayerData(storDriverAccCtx)).zpool,
            asLvIdentifier(vlm),
            vlm.getVolumeDefinition().getVolumeSize(storDriverAccCtx)
        );
    }

    @Override
    protected void deleteLvImpl(Volume vlm, String lvId)
        throws StorageException, AccessDeniedException, SQLException
    {
        ZfsCommands.delete(
            extCmdFactory.create(),
            ((ZfsLayerDataStlt) vlm.getLayerData(storDriverAccCtx)).zpool,
            lvId
        );
    }

    @Override
    public boolean snapshotExists(SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        return ((ZfsLayerDataStlt) snapVlm.getLayerData(storDriverAccCtx)).exists;
    }

    @Override
    protected void createSnapshot(Volume vlm, SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        ZfsCommands.createSnapshot(
            extCmdFactory.create(),
            getStorageName(vlm),
            asLvIdentifier(vlm),
            snapVlm.getSnapshotName().displayValue
        );
    }

    @Override
    protected void restoreSnapshot(String sourceLvId, String sourceSnapshotName, Volume targetVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        ZfsCommands.restoreSnapshot(
            extCmdFactory.create(),
            getZPool(targetVlm),
            sourceLvId,
            sourceSnapshotName,
            asLvIdentifier(targetVlm)
        );
    }

    @Override
    protected void deleteSnapshot(SnapshotVolume snapVlm) throws StorageException, AccessDeniedException, SQLException
    {
        ZfsCommands.delete(
            extCmdFactory.create(),
            ((ZfsLayerDataStlt) snapVlm.getLayerData(storDriverAccCtx)).zpool,
            asLvIdentifier(snapVlm)
        );
    }

    @Override
    protected void rollbackImpl(Volume vlm, String rollbackTargetSnapshotName)
        throws StorageException, AccessDeniedException, SQLException
    {
        ZfsCommands.rollback(
            extCmdFactory.create(),
            getZPool(vlm),
            asLvIdentifier(vlm),
            rollbackTargetSnapshotName
        );
    }

    @Override
    protected String getDevicePath(String zPool, String identifier)
    {
        return String.format(FORMAT_ZFS_DEV_PATH, zPool, identifier);
    }

    @Override
    protected String getIdentifier(ZfsLayerDataStlt layerData)
    {
        return layerData.identifier;
    }

    @Override
    protected Size getSize(ZfsLayerDataStlt layerData)
    {
        return layerData.sizeState;
    }

    @Override
    protected String getStorageName(Volume vlm) throws AccessDeniedException, SQLException
    {
        String volumeGroup = null;
        ZfsLayerDataStlt layerData = (ZfsLayerDataStlt) vlm.getLayerData(storDriverAccCtx);
        if (layerData == null)
        {
            volumeGroup = getZPool(vlm.getStorPool(storDriverAccCtx));
        }
        else
        {
            volumeGroup = layerData.zpool;
        }
        return volumeGroup;
    }

    protected String getStorageName(SnapshotVolume snapVlm) throws AccessDeniedException, SQLException
    {
        String volumeGroup = null;
        ZfsLayerDataStlt layerData = (ZfsLayerDataStlt) snapVlm.getLayerData(storDriverAccCtx);
        if (layerData == null)
        {
            volumeGroup = getZPool(snapVlm.getStorPool(storDriverAccCtx));
        }
        else
        {
            volumeGroup = layerData.zpool;
        }
        return volumeGroup;
    }

    protected String getZPool(StorPool storPool) throws AccessDeniedException
    {
        String zPool;
        try
        {
            zPool = DeviceLayerUtils.getNamespaceStorDriver(
                storPool.getProps(storDriverAccCtx)
            ).getProp(StorageConstants.CONFIG_ZFS_POOL_KEY);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return zPool;
    }

    private String getZPool(Volume vlm) throws AccessDeniedException, SQLException
    {
        String zPool = null;
        ZfsLayerData layerData = (ZfsLayerData) vlm.getLayerData(storDriverAccCtx);
        if (layerData == null)
        {
            zPool = getZPool(vlm.getStorPool(storDriverAccCtx));
        }
        else
        {
            zPool = layerData.getZPool();
        }
        return zPool;
    }

    private String getZPool(SnapshotVolume snapVlm) throws AccessDeniedException, SQLException
    {
        String zPool = null;
        ZfsLayerData layerData = (ZfsLayerData) snapVlm.getLayerData(storDriverAccCtx);
        if (layerData == null)
        {
            zPool = getZPool(snapVlm.getStorPool(storDriverAccCtx));
        }
        else
        {
            zPool = layerData.getZPool();
        }
        return zPool;
    }

    @Override
    public void checkConfig(StorPool storPool) throws StorageException
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String zPool = getZPool(storPool);
        if (zPool == null)
        {
            throw new StorageException("Unset zpool for " + storPool);
        }
        return ZfsUtils.getZPoolTotalSize(
            extCmdFactory.create(),
            Collections.singleton(zPool)
        ).get(zPool);
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String zPool = getZPool(storPool);
        if (zPool == null)
        {
            throw new StorageException("Unset volume group for " + storPool);
        }
        return ZfsUtils.getZPoolFreeSize(
            extCmdFactory.create(),
            Collections.singleton(zPool)
        ).get(zPool);
    }

    @Override
    protected boolean updateDmStats()
    {
        return false;
    }

    @Override
    protected void updateStates(Collection<Volume> vlms, Collection<SnapshotVolume> snapVlms)
        throws StorageException, AccessDeniedException, SQLException
    {
        /*
         *  updating volume states
         */
        for (Volume vlm : vlms)
        {
            ZfsInfo info = infoListCache.get(asFullQualifiedLvIdentifier(vlm));

            ZfsLayerDataStlt state = (ZfsLayerDataStlt) vlm.getLayerData(storDriverAccCtx);
            if (info != null)
            {
                if (state == null)
                {
                    state = createLayerData(vlm, info);
                }
                state.exists = true;

                final long expectedSize = vlm.getVolumeDefinition().getVolumeSize(storDriverAccCtx);
                final long actualSize = info.size;
                if (actualSize != expectedSize)
                {
                    if (actualSize < expectedSize)
                    {
                        state.sizeState = Size.TOO_SMALL;
                    }
                    else
                    {
                        if (actualSize == expectedSize)
                        {
                            state.sizeState = Size.AS_EXPECTED;
                        }
                        else
                        {
                            long extentSize = ZfsUtils.getZfsExtentSize(
                                extCmdFactory.create(),
                                info.poolName,
                                info.identifier
                            );
                            state.sizeState = Size.TOO_LARGE;
                            final long toleratedSize =
                                expectedSize + extentSize * TOLERANCE_FACTOR;
                            if (actualSize < toleratedSize)
                            {
                                state.sizeState = Size.TOO_LARGE_WITHIN_TOLERANCE;
                            }
                        }
                    }
                }
                vlm.setDevicePath(storDriverAccCtx, info.path);
                ProviderUtils.updateSize(vlm, extCmdFactory.create(), storDriverAccCtx);
            }
            else
            {
                if (state == null)
                {
                    state = createEmptyLayerData(vlm);
                }
                state.exists = false;
                vlm.setDevicePath(storDriverAccCtx, null);
                ProviderUtils.setSize(vlm, 0, storDriverAccCtx);
            }
        }

        /*
         *  updating snapshot states
         */
        for (SnapshotVolume snapVlm : snapVlms)
        {
            ZfsInfo info = infoListCache.get(asFullQualifiedLvIdentifier(snapVlm));
            // final VlmStorageState<T> vlmState = vlmStorStateFactory.create((T) info, vlm);

            ZfsLayerDataStlt state = (ZfsLayerDataStlt) snapVlm.getLayerData(storDriverAccCtx);
            if (state == null)
            {
                if (info != null)
                {
                    state = createLayerData(snapVlm, info);
                }
                else
                {
                    state = createEmptyLayerData(snapVlm);
                }
            }
            state.exists = info != null;
        }
    }

    private String asFullQualifiedLvIdentifier(Volume vlm) throws AccessDeniedException, SQLException
    {
        return getStorageName(vlm) + File.separator + asLvIdentifier(vlm);
    }

    protected ZfsLayerDataStlt createLayerData(Volume vlm, ZfsInfo info) throws AccessDeniedException, SQLException
    {
        ZfsLayerDataStlt data = new ZfsLayerDataStlt(info);
        vlm.setLayerData(storDriverAccCtx, data);
        return data;
    }

    protected ZfsLayerDataStlt createLayerData(SnapshotVolume snapvlm, ZfsInfo info)
        throws AccessDeniedException, SQLException
    {
        ZfsLayerDataStlt data = new ZfsLayerDataStlt(info);
        snapvlm.setLayerData(storDriverAccCtx, data);
        return data;
    }

    protected ZfsLayerDataStlt createEmptyLayerData(Volume vlm)
        throws AccessDeniedException, SQLException
    {
        ZfsLayerDataStlt data = new ZfsLayerDataStlt(
            getZPool(vlm),
            asLvIdentifier(vlm),
            -1
        );
        vlm.setLayerData(storDriverAccCtx, data);
        return data;
    }

    protected ZfsLayerDataStlt createEmptyLayerData(SnapshotVolume snapVlm)
        throws AccessDeniedException, SQLException
    {
        ZfsLayerDataStlt data = new ZfsLayerDataStlt(
            getZPool(snapVlm),
            asLvIdentifier(snapVlm.getResourceDefinition().getVolumeDfn(storDriverAccCtx, snapVlm.getVolumeNumber())),
            -1
        );
        snapVlm.setLayerData(storDriverAccCtx, data);
        return data;
    }
}
