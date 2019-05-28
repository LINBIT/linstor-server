package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.AbsStorageProvider;
import com.linbit.linstor.storage.layer.provider.WipeHandler;
import com.linbit.linstor.storage.layer.provider.utils.StorageConfigReader;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.LvmCommands;
import com.linbit.linstor.storage.utils.LvmUtils;
import com.linbit.linstor.storage.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Singleton
public class LvmProvider extends AbsStorageProvider<LvsInfo, LvmData>
{
    private static final int TOLERANCE_FACTOR = 3;
    // FIXME: FORMAT should be private, only made public for LayeredSnapshotHelper
    public static final String FORMAT_RSC_TO_LVM_ID = "%s%s_%05d";
    private static final String FORMAT_LVM_ID_WIPE_IN_PROGRESS = "%s_linstor_wiping_in_progress";
    private static final String FORMAT_DEV_PATH = "/dev/%s/%s";

    private static final String DFLT_LVCREATE_TYPE = "linear";

    protected LvmProvider(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider,
        String subTypeDescr,
        DeviceProviderKind subTypeKind
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
            subTypeKind
        );
    }

    @Inject
    public LvmProvider(
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
            "LVM",
            DeviceProviderKind.LVM
        );
    }

    @Override
    protected void updateStates(List<LvmData> vlmDataList, Collection<SnapshotVolume> snapshots)
        throws StorageException, AccessDeniedException, SQLException
    {
        final Map<String, Long> extentSizes = LvmUtils.getExtentSize(
            extCmdFactory.create(),
            getAffectedVolumeGroups(vlmDataList, snapshots)
        );
        for (LvmData vlmData : vlmDataList)
        {
            final LvsInfo info = infoListCache.get(getFullQualifiedIdentifier(vlmData));
            updateInfo(vlmData, info);

            // final VlmStorageState<T> vlmState = vlmStorStateFactory.create((T) info, vlm);

            if (info != null)
            {
                final long expectedSize = vlmData.getExepectedSize();
                final long actualSize = info.size;
                if (actualSize != expectedSize)
                {
                    if (actualSize < expectedSize)
                    {
                        vlmData.setSizeState(Size.TOO_SMALL);
                    }
                    else
                    {
                        Size sizeState = Size.TOO_LARGE;

                        final long toleratedSize =
                            expectedSize + extentSizes.get(info.volumeGroup) * TOLERANCE_FACTOR;
                        if (actualSize < toleratedSize)
                        {
                            sizeState = Size.TOO_LARGE_WITHIN_TOLERANCE;
                        }
                        vlmData.setSizeState(sizeState);
                    }
                }
                else
                {
                    vlmData.setSizeState(Size.AS_EXPECTED);
                }
            }
        }

        updateSnapshotStates(snapshots);
    }

    private String getFullQualifiedIdentifier(LvmData vlmDataRef)
    {
        return vlmDataRef.getVolumeGroup() +
            File.separator +
            asLvIdentifier(vlmDataRef);
    }

    /*
     * Expected to be overridden (extended) by LvmThinProvider
     */
    protected void updateInfo(LvmData vlmData, LvsInfo info)
        throws AccessDeniedException, SQLException, StorageException
    {
        vlmData.setIdentifier(asLvIdentifier(vlmData));
        if (info == null)
        {
            vlmData.setExists(false);
            vlmData.setVolumeGroup(extractVolumeGroup(vlmData));
            vlmData.setDevicePath(null);
            vlmData.setAllocatedSize(-1);
            vlmData.setUsableSize(-1);
            vlmData.setDevicePath(null);
        }
        else
        {
            vlmData.setExists(true);
            vlmData.setVolumeGroup(info.volumeGroup);
            vlmData.setDevicePath(info.path);
            vlmData.setIdentifier(info.identifier);
            vlmData.setAllocatedSize(info.size);
            vlmData.setUsableSize(info.size);
            vlmData.setDevicePath(info.path);
        }
    }

    protected String extractVolumeGroup(LvmData vlmData) throws AccessDeniedException
    {
        return getVolumeGroup(vlmData.getVolume().getStorPool(storDriverAccCtx));
    }

    /*
     * Expected to be overridden by LvmThinProvider
     */
    @SuppressWarnings("unused")
    protected void updateSnapshotStates(Collection<SnapshotVolume> snapshots)
        throws AccessDeniedException, SQLException
    {
        // no-op
    }

    @Override
    protected void createLvImpl(LvmData vlmData)
        throws StorageException, AccessDeniedException
    {
        LvmCommands.createFat(
            extCmdFactory.create(),
            vlmData.getVolumeGroup(),
            asLvIdentifier(vlmData),
            vlmData.getExepectedSize(),
            "--type=" + getLvCreateType(vlmData)
        );
    }

    protected String getLvCreateType(LvmData vlmDataRef)
    {
        String type;
        try
        {
            Volume vlm = vlmDataRef.getVolume();
            PriorityProps prioProps = new PriorityProps(
                vlm.getProps(storDriverAccCtx),
                vlm.getVolumeDefinition().getProps(storDriverAccCtx),
                vlm.getResourceDefinition().getProps(storDriverAccCtx),
                vlm.getStorPool(storDriverAccCtx).getProps(storDriverAccCtx)
            );
            type = prioProps.getProp(
                ApiConsts.KEY_STOR_POOL_LVCREATE_TYPE,
                ApiConsts.NAMESPC_STORAGE_DRIVER,
                DFLT_LVCREATE_TYPE
            );
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return type;
    }

    @Override
    protected void resizeLvImpl(LvmData vlmData)
        throws StorageException, AccessDeniedException
    {
        LvmCommands.resize(
            extCmdFactory.create(),
            vlmData.getVolumeGroup(),
            asLvIdentifier(vlmData),
            vlmData.getExepectedSize()
        );
    }

    @Override
    protected void deleteLvImpl(LvmData vlmData, String oldLvmId)
        throws StorageException, SQLException
    {
        // just make sure to not colide with any other ongoing wipe-lv-name
        String newLvmId = String.format(FORMAT_LVM_ID_WIPE_IN_PROGRESS, UUID.randomUUID().toString());

        String devicePath = vlmData.getDevicePath();
        // devicePath is the "current" devicePath. as we will rename it right now
        // we will have to adjust the devicePath
        int lastIndexOf = devicePath.lastIndexOf(oldLvmId);
        devicePath = devicePath.substring(0, lastIndexOf) + newLvmId;

        String volumeGroup = vlmData.getVolumeGroup();

        LvmCommands.rename(
            extCmdFactory.create(),
            volumeGroup,
            oldLvmId,
            newLvmId
        );

        wipeHandler.asyncWipe(
            devicePath,
            ignored ->
            {
                try
                {
                    LvmCommands.delete(
                        extCmdFactory.create(),
                        volumeGroup,
                        newLvmId
                    );
                    vlmData.setExists(false);
                }
                catch (StorageException exc)
                {
                    errorReporter.reportError(exc);
                }
                catch (SQLException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
        );
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        Map<String, Long> freeSizes = LvmUtils.getVgFreeSize(extCmdFactory.create(), changedStoragePoolStrings);
        for (String storPool : changedStoragePoolStrings)
        {
            if (!freeSizes.containsKey(storPool))
            {
                freeSizes.put(storPool, SIZE_OF_NOT_FOUND_STOR_POOL);
            }
        }
        return freeSizes;
    }

    @Override
    protected Map<String, LvsInfo> getInfoListImpl(List<LvmData> vlmDataList, List<SnapshotVolume> snapVlms)
        throws StorageException, AccessDeniedException
    {
        return LvmUtils.getLvsInfo(
            extCmdFactory.create(),
            getAffectedVolumeGroups(vlmDataList, snapVlms)
        );
    }

    @Override
    protected String getDevicePath(String storageName, String lvId)
    {
        return String.format(FORMAT_DEV_PATH, storageName, lvId);
    }

    @Override
    protected String asLvIdentifier(ResourceName resourceName, String rscNameSuffix, VolumeNumber volumeNumber)
    {
        return String.format(
            FORMAT_RSC_TO_LVM_ID,
            resourceName.displayValue,
            rscNameSuffix,
            volumeNumber.value
        );
    }

    @Override
    protected String getStorageName(StorPool storPoolRef)
    {
        return getVolumeGroup(storPoolRef);
    }

    protected String getVolumeGroup(StorPool storPool)
    {
        String volumeGroup;
        try
        {
            volumeGroup = DeviceLayerUtils.getNamespaceStorDriver(
                    storPool.getProps(storDriverAccCtx)
                )
                .getProp(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY);
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return volumeGroup;
    }

    @Override
    protected boolean updateDmStats()
    {
        return true; // LVM driver should call dmstats commands
    }

    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String vg = getVolumeGroup(storPool);
        if (vg == null)
        {
            throw new StorageException("Unset volume group for " + storPool);
        }
        Long capacity = LvmUtils.getVgTotalSize(
            extCmdFactory.create(),
            Collections.singleton(vg)
        ).get(vg);
        return capacity == null ? SIZE_OF_NOT_FOUND_STOR_POOL : capacity;
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String vg = getVolumeGroup(storPool);
        if (vg == null)
        {
            throw new StorageException("Unset volume group for " + storPool);
        }
        Long freespace = LvmUtils.getVgFreeSize(
            extCmdFactory.create(),
            Collections.singleton(vg)
        ).get(vg);
        return freespace == null ? SIZE_OF_NOT_FOUND_STOR_POOL : freespace;
    }

    /*
     * Expected to be overridden by LvmThinProvider (maybe additionally called)
     */
    @Override
    public void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException
    {
        Props props = DeviceLayerUtils.getNamespaceStorDriver(
            storPool.getProps(storDriverAccCtx)
        );
        StorageConfigReader.checkVolumeGroupEntry(extCmdFactory.create(), props);
        StorageConfigReader.checkToleranceFactor(props);
    }

    private Set<String> getAffectedVolumeGroups(
        Collection<LvmData> vlmDataList,
        Collection<SnapshotVolume> snapVlms
    )
        throws AccessDeniedException
    {
        Set<String> volumeGroups = new HashSet<>();
        for (LvmData vlmData : vlmDataList)
        {
            String volumeGroup = vlmData.getVolumeGroup();
            if (volumeGroup == null)
            {
                volumeGroup = getVolumeGroup(vlmData.getVolume().getStorPool(storDriverAccCtx));
                vlmData.setVolumeGroup(volumeGroup);
            }
            if (volumeGroup != null)
            {
                volumeGroups.add(volumeGroup);
            }
        }
        for (SnapshotVolume snapVlm : snapVlms)
        {
            volumeGroups.add(getVolumeGroup(snapVlm.getStorPool(storDriverAccCtx)));
        }
        return volumeGroups;
    }

    @Override
    protected void setDevicePath(LvmData vlmData, String devPath) throws SQLException
    {
        vlmData.setDevicePath(devPath);
    }

    @Override
    protected void setAllocatedSize(LvmData vlmData, long size) throws SQLException
    {
        vlmData.setAllocatedSize(size);
    }

    @Override
    protected void setUsableSize(LvmData vlmData, long size) throws SQLException
    {
        vlmData.setUsableSize(size);
    }

    @Override
    protected void setExpectedUsableSize(LvmData vlmData, long size)
    {
        vlmData.setExepectedSize(size);
    }

    @Override
    protected String getStorageName(LvmData vlmDataRef) throws SQLException
    {
        return vlmDataRef.getVolumeGroup();
    }
}
