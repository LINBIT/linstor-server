package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.AbsStorageProvider;
import com.linbit.linstor.storage.layer.provider.WipeHandler;
import com.linbit.linstor.storage.layer.provider.utils.StorageConfigReader;
import com.linbit.linstor.storage.layer.provider.utils.ProviderUtils;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.LvmCommands;
import com.linbit.linstor.storage.utils.LvmUtils;
import com.linbit.linstor.storage.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

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
    public static final String FORMAT_RSC_TO_LVM_ID = "%s_%s_%05d";
    private static final String FORMAT_LVM_ID_WIPE_IN_PROGRESS = "%s_linstor_wiping_in_progress";
    private static final String FORMAT_DEV_PATH = "/dev/%s/%s";

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
            getAffectedVolumeGroups(vlmDataList)
        );
        for (LvmData vlmData : vlmDataList)
        {
            vlmData.identifier = asLvIdentifier(vlmData);
            final LvsInfo info = infoListCache.get(vlmData.identifier);

            // final VlmStorageState<T> vlmState = vlmStorStateFactory.create((T) info, vlm);

            if (info != null)
            {
                vlmData.exists = true;
                vlmData.updateInfo(info);

                final long expectedSize = vlmData.getUsableSize();
                final long actualSize = info.size;
                if (actualSize != expectedSize)
                {
                    if (actualSize < expectedSize)
                    {
                        vlmData.sizeState = Size.TOO_SMALL;
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
                        vlmData.sizeState = sizeState;
                    }
                }
                // vlm.setUsableSize already set in StorageLayer#updateGrossSize
                vlmData.allocatedSize = ProviderUtils.getAllocatedSize(vlmData, extCmdFactory.create());
            }
            else
            {
                extractVolumeGroup(vlmData);
                vlmData.exists = false;
                vlmData.devicePath = null;
                vlmData.allocatedSize = -1;
            }
        }

        updateSnapshotStates(snapshots);
    }

    /*
     * Expected to be overridden by LvmThinProvider
     */
    protected void extractVolumeGroup(LvmData vlmData) throws AccessDeniedException
    {
        vlmData.volumeGroup = getVolumeGroup(vlmData.vlm.getStorPool(storDriverAccCtx));
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
        throws StorageException
    {
        LvmCommands.createFat(
            extCmdFactory.create(),
            vlmData.getVolumeGroup(),
            asLvIdentifier(vlmData),
            vlmData.usableSize
        );
    }

    @Override
    protected void resizeLvImpl(LvmData vlmData)
        throws StorageException
    {
        LvmCommands.resize(
            extCmdFactory.create(),
            vlmData.volumeGroup,
            asLvIdentifier(vlmData),
            vlmData.usableSize
        );
    }

    @Override
    protected void deleteLvImpl(LvmData vlmData, String oldLvmId)
        throws StorageException
    {
        // just make sure to not colide with any other ongoing wipe-lv-name
        String newLvmId = String.format(FORMAT_LVM_ID_WIPE_IN_PROGRESS, UUID.randomUUID().toString());

        String devicePath = vlmData.devicePath;
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
                    vlmData.exists = false;
                }
                catch (StorageException exc)
                {
                    errorReporter.reportError(exc);
                }
            }
        );
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        return LvmUtils.getVgFreeSize(extCmdFactory.create(), changedStoragePoolStrings);
    }

    @Override
    protected Map<String, LvsInfo> getInfoListImpl(List<LvmData> vlmDataList)
        throws StorageException, AccessDeniedException
    {
        return LvmUtils.getLvsInfo(extCmdFactory.create(), getAffectedVolumeGroups(vlmDataList));
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
    protected String getIdentifier(LvmData layerData)
    {
        return layerData.identifier;
    }

    @Override
    protected Size getSize(LvmData layerData)
    {
        return layerData.sizeState;
    }

    @Override
    protected String getStorageName(LvmData vlmData) throws AccessDeniedException, SQLException
    {
        return vlmData.volumeGroup;
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
        return LvmUtils.getVgTotalSize(
            extCmdFactory.create(),
            Collections.singleton(vg)
        ).get(vg);
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String vg = getVolumeGroup(storPool);
        if (vg == null)
        {
            throw new StorageException("Unset volume group for " + storPool);
        }
        return LvmUtils.getVgFreeSize(
            extCmdFactory.create(),
            Collections.singleton(vg)
        ).get(vg);
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

    private Set<String> getAffectedVolumeGroups(List<LvmData> vlmDataList) throws AccessDeniedException
    {
        Set<String> volumeGroups = new HashSet<>();
        for (LvmData vlmData : vlmDataList)
        {
            String volumeGroup = vlmData.volumeGroup;
            if (volumeGroup == null)
            {
                volumeGroup = getVolumeGroup(vlmData.vlm.getStorPool(storDriverAccCtx));
                vlmData.volumeGroup = volumeGroup;
            }
            volumeGroups.add(volumeGroup);
        }
        return volumeGroups;
    }

    @Override
    protected void setDevicePath(LvmData vlmData, String devPath)
    {
        vlmData.devicePath = devPath;
    }

    @Override
    protected void setAllocatedSize(LvmData vlmData, long size)
    {
        vlmData.allocatedSize = size;
    }

    @Override
    protected void setUsableSize(LvmData vlmData, long size)
    {
        vlmData.usableSize = size;
    }
}
