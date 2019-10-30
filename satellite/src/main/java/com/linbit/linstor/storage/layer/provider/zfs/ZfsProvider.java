package com.linbit.linstor.storage.layer.provider.zfs;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.AbsStorageProvider;
import com.linbit.linstor.storage.layer.provider.WipeHandler;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.PmemUtils;
import com.linbit.linstor.storage.utils.ZfsCommands;
import com.linbit.linstor.storage.utils.ZfsUtils;
import com.linbit.linstor.storage.utils.ZfsUtils.ZfsInfo;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
    public static final String FORMAT_RSC_TO_ZFS_ID = "%s%s_%05d";
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
    protected Map<String, ZfsInfo> getInfoListImpl(
        List<ZfsData> vlmDataListRef,
        List<SnapshotVolume> snapVlmsRef
    )
        throws StorageException
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

    private List<String> asFullQualifiedLvIdentifierList(String rscNameSuffix, SnapshotVolume snapVlm)
        throws AccessDeniedException
    {
        List<String> fqLvIdList = new ArrayList<>();

        fqLvIdList.add(
            getZPool(snapVlm.getStorPool(storDriverAccCtx)) + File.separator +
                asLvIdentifier(rscNameSuffix, snapVlm)
        );

        return fqLvIdList;
    }

    private String asLvIdentifier(String rscNameSuffix, SnapshotVolume snapVlm)
    {
        return asLvIdentifier(rscNameSuffix, snapVlm.getSnapshotVolumeDefinition()) + "@" +
            snapVlm.getSnapshotName().displayValue;
    }

    @Override
    protected void createLvImpl(ZfsData vlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.create(
            extCmdFactory.create(),
            vlmData.getZPool(),
            asLvIdentifier(vlmData),
            vlmData.getExepectedSize(),
            false
        );
    }

    protected long roundUpToExtentSize(long sizeRef)
    {
        long volumeSize = sizeRef;
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
        }
        return volumeSize;
    }

    @Override
    protected void resizeLvImpl(ZfsData vlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.resize(
            extCmdFactory.create(),
            vlmData.getZPool(),
            asLvIdentifier(vlmData),
            vlmData.getExepectedSize()
        );
    }

    @Override
    protected void deleteLvImpl(ZfsData vlmData, String lvId)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.delete(
            extCmdFactory.create(),
            vlmData.getZPool(),
            lvId
        );
        vlmData.setExists(false);
    }

    @Override
    public boolean snapshotExists(SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // FIXME: RAID: rscNameSuffix

        boolean oneExists = false;
        boolean allExsits = true;
        List<String> identifierList = asFullQualifiedLvIdentifierList("", snapVlm);

        for (String snapLvId : identifierList)
        {
            if (infoListCache.get(snapLvId) != null)
            {
                oneExists = true;
            }
            else
            {
                allExsits = false;
            }
        }
        if (oneExists && !allExsits)
        {
            // FIXME: what the heck should we do now?
            errorReporter.logError("Some, but not all zfs volumes of snapshot " + snapVlm + " exist.");
        }
        return allExsits;
    }

    @Override
    protected void createSnapshot(ZfsData vlmData, SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.createSnapshot(
            extCmdFactory.create(),
            vlmData.getZPool(),
            asLvIdentifier(vlmData),
            snapVlm.getSnapshotName().displayValue
        );
    }

    @Override
    protected void restoreSnapshot(String sourceLvId, String sourceSnapName, ZfsData targetVlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.restoreSnapshot(
            extCmdFactory.create(),
            targetVlmData.getZPool(),
            sourceLvId,
            sourceSnapName,
            asLvIdentifier(targetVlmData)
        );
    }


    @Override
    protected void deleteSnapshot(String rscNameSuffix, SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.delete(
            extCmdFactory.create(),
            getZPool(snapVlm.getStorPool(storDriverAccCtx)),
            asLvIdentifier(rscNameSuffix, snapVlm)
        );
    }

    @Override
    protected void rollbackImpl(ZfsData vlmData, String rollbackTargetSnapshotName)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.rollback(
            extCmdFactory.create(),
            vlmData.getZPool(),
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
    protected String getStorageName(StorPool storPoolRef) throws AccessDeniedException
    {
        return getZPool(storPoolRef);
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
        HashMap<String, ZfsInfo> zfsPoolMap = ZfsUtils.getThinZPoolsList(extCmdFactory.create());
        if (!zfsPoolMap.containsKey(zpoolName))
        {
            throw new StorageException("no zpool found with name '" + zpoolName + "'");
        }
    }

    @Override
    public void update(StorPool storPoolRef) throws AccessDeniedException, DatabaseException, StorageException
    {
        List<String> pvs = ZfsUtils.getPhysicalVolumes(extCmdFactory.create(), getZpoolOnlyName(storPoolRef));
        if (PmemUtils.supportsDax(extCmdFactory.create(), pvs))
        {
            storPoolRef.setPmem(true);
        }
    }

    protected String getZpoolOnlyName(StorPool storPoolRef) throws AccessDeniedException, StorageException
    {
        String zpoolName = getZPool(storPoolRef);
        if (zpoolName == null)
        {
            throw new StorageException("Unset zfs dataset for " + storPoolRef);
        }

        int idx = zpoolName.indexOf(File.separator);
        if (idx == -1)
        {
            idx = zpoolName.length();
        }
        return zpoolName.substring(0, idx);
    }

    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String poolName = getZPool(storPool);
        if (poolName == null)
        {
            throw new StorageException("Unset zfs dataset for " + storPool);
        }

        int idx = poolName.indexOf(File.separator);
        if (idx == -1)
        {
            idx = poolName.length();
        }
        String rootPoolName = poolName.substring(0, idx);

        // do not use sub pool, we have to ask the actual zpool, not the sub dataset
        return ZfsUtils.getZPoolTotalSize(
            extCmdFactory.create(),
            Collections.singleton(rootPoolName)
        ).get(rootPoolName);
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String zPool = getZPool(storPool);
        if (zPool == null)
        {
            throw new StorageException("Unset zpool for " + storPool);
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
        throws StorageException, AccessDeniedException, DatabaseException
    {
        Set<StorPool> storPools = new TreeSet<>();
        /*
         *  updating volume states
         */
        for (ZfsData vlmData : vlmDataList)
        {
            storPools.add(vlmData.getStorPool());

            vlmData.setZPool(getZPool(vlmData.getStorPool()));
            vlmData.setIdentifier(asLvIdentifier(vlmData));
            ZfsInfo info = infoListCache.get(vlmData.getFullQualifiedLvIdentifier());

            if (info != null)
            {
                updateInfo(vlmData, info);

                final long expectedSize = vlmData.getExepectedSize();
                final long actualSize = info.allocatedSize;
                if (actualSize != expectedSize)
                {
                    if (actualSize < expectedSize)
                    {
                        vlmData.setSizeState(Size.TOO_SMALL);
                    }
                    else
                    {
                        long extentSize = ZfsUtils.getZfsExtentSize(
                            extCmdFactory.create(),
                            info.poolName,
                            info.identifier
                        );
                        vlmData.setSizeState(Size.TOO_LARGE);
                        final long toleratedSize =
                            expectedSize + extentSize * TOLERANCE_FACTOR;
                        if (actualSize < toleratedSize)
                        {
                            vlmData.setSizeState(Size.TOO_LARGE_WITHIN_TOLERANCE);
                        }
                    }
                }
                else
                {
                    vlmData.setSizeState(Size.AS_EXPECTED);
                }
            }
            else
            {
                vlmData.setExists(false);
                vlmData.setDevicePath(null);
                vlmData.setAllocatedSize(-1);
            }
        }
    }

    private void updateInfo(ZfsData vlmData, ZfsInfo zfsInfo) throws DatabaseException
    {
        vlmData.setExists(true);
        vlmData.setZPool(zfsInfo.poolName);
        vlmData.setIdentifier(zfsInfo.identifier);
        vlmData.setAllocatedSize(zfsInfo.allocatedSize);
        vlmData.setUsableSize(zfsInfo.usableSize);
        vlmData.setDevicePath(zfsInfo.path);
    }

    @Override
    protected void setDevicePath(ZfsData vlmDataRef, String devicePathRef) throws DatabaseException
    {
        vlmDataRef.setDevicePath(devicePathRef);
    }

    @Override
    protected void setAllocatedSize(ZfsData vlmDataRef, long sizeRef) throws DatabaseException
    {
        vlmDataRef.setAllocatedSize(sizeRef);
    }

    @Override
    protected void setUsableSize(ZfsData vlmDataRef, long sizeRef) throws DatabaseException
    {
        vlmDataRef.setUsableSize(sizeRef);
    }

    @Override
    protected void setExpectedUsableSize(ZfsData vlmData, long size) throws DatabaseException
    {
        vlmData.setExepectedSize(
            roundUpToExtentSize(size)
        );
    }

    @Override
    protected String getStorageName(ZfsData vlmDataRef) throws DatabaseException
    {
        return vlmDataRef.getZPool();
    }
}
