package com.linbit.linstor.storage.layer.provider.spdk;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.*;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.spdk.SpdkData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.AbsStorageProvider;
import com.linbit.linstor.storage.layer.provider.WipeHandler;
import com.linbit.linstor.storage.layer.provider.utils.SpdkConfigReader;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.SpdkCommands;
import com.linbit.linstor.storage.utils.SpdkUtils;
import com.linbit.linstor.storage.utils.SpdkUtils.LvsInfo;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.File;
import java.util.*;

import static com.linbit.linstor.storage.utils.SpdkUtils.SPDK_PATH_PREFIX;

@Singleton
public class SpdkProvider extends AbsStorageProvider<LvsInfo, SpdkData>
{
    private static final int TOLERANCE_FACTOR = 3;
    // FIXME: FORMAT should be private, only made public for LayeredSnapshotHelper
    public static final String FORMAT_RSC_TO_SPDK_ID = "%s%s_%05d";
    private static final String FORMAT_SPDK_ID_WIPE_IN_PROGRESS = "%s-linstor_wiping_in_progress";
    private static final String SPDK_FORMAT_DEV_PATH = SPDK_PATH_PREFIX+"%s/%s";

    private static final String DFLT_LVCREATE_TYPE = "linear";

    protected SpdkProvider(
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
    public SpdkProvider(
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
            "SPDK",
            DeviceProviderKind.SPDK
        );
    }

    @Override
    protected void updateStates(List<SpdkData> vlmDataList, Collection<SnapshotVolume> snapshots)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        final Map<String, Long> extentSizes = SpdkUtils.getExtentSize(
            extCmdFactory.create(),
            getAffectedVolumeGroups(vlmDataList, snapshots)
        );
        for (SpdkData vlmData : vlmDataList)
        {

            final LvsInfo info = infoListCache.get(getFullQualifiedIdentifier(vlmData));
            updateInfo(vlmData, info);

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

    private String getFullQualifiedIdentifier(SpdkData vlmDataRef)
    {
        return vlmDataRef.getVolumeGroup() +
            File.separator +
            asLvIdentifier(vlmDataRef);
    }

    protected void updateInfo(SpdkData vlmData, LvsInfo info)
        throws DatabaseException, AccessDeniedException, StorageException
    {
        vlmData.setIdentifier(asLvIdentifier(vlmData));
        if (info == null)
        {
            vlmData.setExists(false);
            vlmData.setVolumeGroup(extractVolumeGroup(vlmData));
            vlmData.setDevicePath(null);
            vlmData.setAllocatedSize(-1);
            // vlmData.setUsableSize(-1);
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
        }
    }

    protected String extractVolumeGroup(SpdkData vlmData)
    {
        return getVolumeGroup(vlmData.getStorPool());
    }

    @SuppressWarnings("unused")
    protected void updateSnapshotStates(Collection<SnapshotVolume> snapshots)
        throws AccessDeniedException, DatabaseException
    {
        // no-op
    }

    @Override
    protected void createLvImpl(SpdkData vlmData)
        throws StorageException, AccessDeniedException
    {
        SpdkCommands.createFat(
            extCmdFactory.create(),
            vlmData.getVolumeGroup(),
            asLvIdentifier(vlmData),
            vlmData.getExepectedSize(),
            "--type=" + getLvCreateType(vlmData)
        );
    }

    protected String getLvCreateType(SpdkData vlmDataRef)
    {
        String type;
        try
        {
            Volume vlm = vlmDataRef.getVolume();
            ResourceDefinition rscDfn = vlm.getResourceDefinition();
            ResourceGroup rscGrp = rscDfn.getResourceGroup();
            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
            PriorityProps prioProps = new PriorityProps(
                vlm.getProps(storDriverAccCtx),
                vlmDfn.getProps(storDriverAccCtx),
                rscGrp.getVolumeGroupProps(storDriverAccCtx, vlmDfn.getVolumeNumber()),
                rscDfn.getProps(storDriverAccCtx),
                rscGrp.getProps(storDriverAccCtx),
                vlmDataRef.getStorPool().getProps(storDriverAccCtx)
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
    protected void resizeLvImpl(SpdkData vlmData)
        throws StorageException, AccessDeniedException
    {
        SpdkCommands.resize(
            extCmdFactory.create(),
            vlmData.getVolumeGroup(),
            asLvIdentifier(vlmData),
            vlmData.getExepectedSize()
        );
    }

    @Override
    protected void deleteLvImpl(SpdkData vlmData, String oldSpdkId)
        throws StorageException, DatabaseException
    {
        // just make sure to not colide with any other ongoing wipe-lv-name
        String newSpdkId = String.format(
            FORMAT_SPDK_ID_WIPE_IN_PROGRESS,
            asLvIdentifier(vlmData)
        );

        String devicePath = vlmData.getDevicePath();
        // devicePath is the "current" devicePath. as we will rename it right now
        // we will have to adjust the devicePath
        int lastIndexOf = devicePath.lastIndexOf(oldSpdkId);
        devicePath = devicePath.substring(0, lastIndexOf) + newSpdkId;
        String volumeGroup = vlmData.getVolumeGroup();

        SpdkCommands.rename(
            extCmdFactory.create(),
            volumeGroup,
            oldSpdkId,
            newSpdkId
        );

        vlmData.setExists(false);

        //SPDK by default wipes a lvol bdev during deletion, unless this option was disabled in lvol store
        try
        {
            SpdkCommands.delete(
                    extCmdFactory.create(),
                    volumeGroup,
                    newSpdkId
            );
        }
                catch (StorageException exc)
        {
            errorReporter.reportError(exc);
        }

    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        Map<String, Long> freeSizes = SpdkUtils.getVgFreeSize(extCmdFactory.create(), changedStoragePoolStrings);
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
    protected Map<String, LvsInfo> getInfoListImpl(List<SpdkData> vlmDataList, List<SnapshotVolume> snapVlms)
        throws StorageException, AccessDeniedException
    {
        return SpdkUtils.getLvsInfo(
            extCmdFactory.create(),
            getAffectedVolumeGroups(vlmDataList, snapVlms)
        );
    }

    @Override
    protected String getDevicePath(String storageName, String lvId)
    {
        return String.format(SPDK_FORMAT_DEV_PATH, storageName, lvId);
    }

    @Override
    protected String asLvIdentifier(ResourceName resourceName, String rscNameSuffix, VolumeNumber volumeNumber)
    {
        return String.format(
            FORMAT_RSC_TO_SPDK_ID,
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
        return false; // no path to a block device in SPDK
    }

    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String vg = getVolumeGroup(storPool);
        if (vg == null)
        {
            throw new StorageException("Unset volume group for " + storPool);
        }
        Long capacity = SpdkUtils.getVgTotalSize(
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
        Long freespace = SpdkUtils.getVgFreeSize(
            extCmdFactory.create(),
            Collections.singleton(vg)
        ).get(vg);
        return freespace == null ? SIZE_OF_NOT_FOUND_STOR_POOL : freespace;
    }

    @Override
    public void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException
    {
        Props props = DeviceLayerUtils.getNamespaceStorDriver(
            storPool.getProps(storDriverAccCtx)
        );
        SpdkConfigReader.checkVolumeGroupEntry(extCmdFactory.create(), props);
        SpdkConfigReader.checkToleranceFactor(props);
    }

    private Set<String> getAffectedVolumeGroups(
        Collection<SpdkData> vlmDataList,
        Collection<SnapshotVolume> snapVlms
    )
        throws AccessDeniedException
    {
        Set<String> volumeGroups = new HashSet<>();
        for (SpdkData vlmData : vlmDataList)
        {
            String volumeGroup = vlmData.getVolumeGroup();
            if (volumeGroup == null)
            {
                volumeGroup = getVolumeGroup(vlmData.getStorPool());
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
    protected void setDevicePath(SpdkData vlmData, String devPath) throws DatabaseException
    {
        vlmData.setDevicePath(devPath);
    }

    @Override
    protected void setAllocatedSize(SpdkData vlmData, long size) throws DatabaseException
    {
        vlmData.setAllocatedSize(size);
    }

    @Override
    protected void setUsableSize(SpdkData vlmData, long size) throws DatabaseException
    {
        vlmData.setUsableSize(size);
    }

    @Override
    protected void setExpectedUsableSize(SpdkData vlmData, long size)
    {
        vlmData.setExepectedSize(size);
    }

    @Override
    protected String getStorageName(SpdkData vlmDataRef) throws DatabaseException
    {
        return vlmDataRef.getVolumeGroup();
    }
}
