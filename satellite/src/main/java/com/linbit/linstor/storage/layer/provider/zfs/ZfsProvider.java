package com.linbit.linstor.storage.layer.provider.zfs;

import com.linbit.Checks;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.AbsStorageProvider;
import com.linbit.linstor.storage.layer.provider.WipeHandler;
import com.linbit.linstor.storage.layer.provider.utils.ProviderUtils;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.ZfsCommands;
import com.linbit.linstor.storage.utils.ZfsUtils;
import com.linbit.linstor.storage.utils.ZfsUtils.ZfsInfo;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

@Singleton
public class ZfsProvider extends AbsStorageProvider<ZfsInfo, ZfsData>
{
    protected static final int DEFAULT_ZFS_EXTENT_SIZE = 8; // 8K

    // FIXME: FORMAT should be private, only made public for LayeredSnapshotHelper
    public static final String FORMAT_RSC_TO_ZFS_ID = "%s_%s_%05d";
    private static final String FORMAT_ZFS_DEV_PATH = "/dev/zvol/%s/%s";
    private static final int TOLERANCE_FACTOR = 3;

    private Map<StorPool, Long> extentSizes = new TreeMap<>();

    protected ZfsProvider(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider,
        String subTypeDescr,
        DeviceProviderKind kind
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
            subTypeDescr,
            kind
        );
    }

    @Inject
    public ZfsProvider(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        @DeviceManagerContext AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider
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
            "ZFS",
            DeviceProviderKind.ZFS
        );
    }

    @Override
    public void clearCache() throws StorageException
    {
        super.clearCache();
        extentSizes.clear();
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        return ZfsUtils.getZPoolFreeSize(extCmdFactory.create(), changedStoragePoolStrings);
    }

    @Override
    protected Map<String, ZfsInfo> getInfoListImpl(List<ZfsData> vlmDataListRef) throws StorageException
    {
        return ZfsUtils.getZfsList(extCmdFactory.create());
    }

    @Override
    protected String asLvIdentifier(ResourceName resourceName, String rscNameSuffix, VolumeNumber volumeNumber)
    {
        return String.format(
            FORMAT_RSC_TO_ZFS_ID,
            resourceName.displayValue,
            rscNameSuffix,
            volumeNumber.value
        );
    }

    private String asFullQualifiedLvIdentifier(String rscNameSuffix, SnapshotVolume snapVlm)
        throws AccessDeniedException
    {
        return getZPool(snapVlm.getStorPool(storDriverAccCtx)) + File.separator +
            asLvIdentifier(rscNameSuffix, snapVlm);
    }

    private String asLvIdentifier(String rscNameSuffix, SnapshotVolume snapVlm)
    {
        return asLvIdentifier(rscNameSuffix, snapVlm.getSnapshotVolumeDefinition()) + "@" +
            snapVlm.getSnapshotName().displayValue;
    }

    @Override
    protected void createLvImpl(ZfsData vlmData)
        throws StorageException, AccessDeniedException, SQLException
    {
        long volumeSize = roundUpToExtentSize(vlmData);

        ZfsCommands.create(
            extCmdFactory.create(),
            vlmData.zpool,
            asLvIdentifier(vlmData),
            volumeSize,
            false
        );
    }

    protected long roundUpToExtentSize(ZfsData vlmData)
    {
        long volumeSize = vlmData.usableSize;
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
            vlmData.allocatedSize = volumeSize;
        }
        return volumeSize;
    }

    @Override
    protected void resizeLvImpl(ZfsData vlmData)
        throws StorageException, AccessDeniedException, SQLException
    {
        ZfsCommands.resize(
            extCmdFactory.create(),
            vlmData.zpool,
            asLvIdentifier(vlmData),
            vlmData.usableSize
        );
    }

    @Override
    protected void deleteLvImpl(ZfsData vlmData, String lvId)
        throws StorageException, AccessDeniedException, SQLException
    {
        ZfsCommands.delete(
            extCmdFactory.create(),
            vlmData.zpool,
            lvId
        );
        vlmData.exists = false;
    }

    @Override
    public boolean snapshotExists(SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        // FIXME: RAID: rscNameSuffix
        return infoListCache.get(asFullQualifiedLvIdentifier("", snapVlm)) != null;
    }

    @Override
    protected void createSnapshot(ZfsData vlmData, SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        ZfsCommands.createSnapshot(
            extCmdFactory.create(),
            getStorageName(vlmData),
            asLvIdentifier(vlmData),
            snapVlm.getSnapshotName().displayValue
        );
    }

    @Override
    protected void restoreSnapshot(String sourceLvId, String sourceSnapName, ZfsData targetVlmData)
        throws StorageException, AccessDeniedException, SQLException
    {
        ZfsCommands.restoreSnapshot(
            extCmdFactory.create(),
            getStorageName(targetVlmData),
            sourceLvId,
            sourceSnapName,
            asLvIdentifier(targetVlmData)
        );
    }


    @Override
    protected void deleteSnapshot(String rscNameSuffix, SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        ZfsCommands.delete(
            extCmdFactory.create(),
            getZPool(snapVlm.getStorPool(storDriverAccCtx)),
            asLvIdentifier(rscNameSuffix, snapVlm)
        );
    }

    @Override
    protected void rollbackImpl(ZfsData vlmData, String rollbackTargetSnapshotName)
        throws StorageException, AccessDeniedException, SQLException
    {
        ZfsCommands.rollback(
            extCmdFactory.create(),
            getStorageName(vlmData),
            asLvIdentifier(vlmData),
            rollbackTargetSnapshotName
        );
    }

    @Override
    protected String getDevicePath(String zPool, String identifier)
    {
        return String.format(FORMAT_ZFS_DEV_PATH, zPool, identifier);
    }

    @Override
    protected String getIdentifier(ZfsData layerData)
    {
        return layerData.identifier;
    }

    @Override
    protected Size getSize(ZfsData layerData)
    {
        return layerData.sizeState;
    }

    @Override
    protected String getStorageName(ZfsData vlmData)
    {
        return vlmData.zpool;
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

    @Override
    public void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String zpoolName = getZPool(storPool);
        if (zpoolName == null)
        {
            throw new StorageException(
                "zPool name not given for storPool '" +
                    storPool.getName().displayValue + "'"
            );
        }
        zpoolName = zpoolName.trim();
        try
        {
            Checks.nameCheck(
                zpoolName,
                1,
                Integer.MAX_VALUE,
                StorPoolName.VALID_CHARS,
                StorPoolName.VALID_INNER_CHARS
            );
        }
        catch (InvalidNameException ine)
        {
            final String cause = String.format("Invalid pool name: %s", zpoolName);
            throw new StorageException(
                "Invalid configuration, " + cause,
                null,
                cause,
                "Specify a valid and existing pool name",
                null
            );
        }

        Set<String> zpoolList = ZfsUtils.getZPoolList(extCmdFactory.create());
        if (!zpoolList.contains(zpoolName))
        {
            throw new StorageException("no zpool found with name '" + zpoolName + "'");
        }
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
    protected void updateStates(List<ZfsData> vlmDataList, Collection<SnapshotVolume> snapVlms)
        throws StorageException, AccessDeniedException, SQLException
    {
        Set<StorPool> storPools = new TreeSet<>();
        /*
         *  updating volume states
         */
        for (ZfsData vlmData : vlmDataList)
        {
            storPools.add(vlmData.vlm.getStorPool(storDriverAccCtx));

            vlmData.zpool = getZPool(vlmData.vlm.getStorPool(storDriverAccCtx));
            vlmData.identifier = asFullQualifiedLvIdentifier(vlmData);
            ZfsInfo info = infoListCache.get(vlmData.identifier);

            if (info != null)
            {
                vlmData.updateInfo(info);

                final long expectedSize = vlmData.usableSize;
                final long actualSize = info.size;
                if (actualSize != expectedSize)
                {
                    if (actualSize < expectedSize)
                    {
                        vlmData.sizeState = Size.TOO_SMALL;
                    }
                    else
                    {
                        if (actualSize == expectedSize)
                        {
                            vlmData.sizeState = Size.AS_EXPECTED;
                        }
                        else
                        {
                            long extentSize = ZfsUtils.getZfsExtentSize(
                                extCmdFactory.create(),
                                info.poolName,
                                info.identifier
                            );
                            vlmData.sizeState = Size.TOO_LARGE;
                            final long toleratedSize =
                                expectedSize + extentSize * TOLERANCE_FACTOR;
                            if (actualSize < toleratedSize)
                            {
                                vlmData.sizeState = Size.TOO_LARGE_WITHIN_TOLERANCE;
                            }
                        }
                    }
                }
                vlmData.allocatedSize = ProviderUtils.getAllocatedSize(vlmData, extCmdFactory.create());
            }
            else
            {
                vlmData.exists = false;
                vlmData.devicePath = null;
                vlmData.allocatedSize = -1;
            }
        }
    }

    private String asFullQualifiedLvIdentifier(ZfsData vlmData)
    {
        return getStorageName(vlmData) + File.separator + asLvIdentifier(vlmData);
    }

    @Override
    protected void setDevicePath(ZfsData vlmData, String devPath)
    {
        vlmData.devicePath = devPath;
    }

    @Override
    protected void setAllocatedSize(ZfsData vlmData, long size)
    {
        vlmData.allocatedSize = size;
    }

    @Override
    protected void setUsableSize(ZfsData vlmData, long size)
    {
        vlmData.usableSize = size;
    }
}
