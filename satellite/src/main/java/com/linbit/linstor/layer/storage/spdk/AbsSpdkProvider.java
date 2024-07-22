package com.linbit.linstor.layer.storage.spdk;

import com.linbit.ImplementationError;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.DeviceLayerUtils;
import com.linbit.linstor.layer.nvme.NvmeUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.storage.spdk.utils.SpdkConfigReader;
import com.linbit.linstor.layer.storage.spdk.utils.SpdkUtils;
import com.linbit.linstor.layer.storage.spdk.utils.SpdkUtils.LvsInfo;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.spdk.SpdkData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import static com.linbit.linstor.layer.storage.spdk.utils.SpdkUtils.SPDK_PATH_PREFIX;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbsSpdkProvider<T> extends AbsStorageProvider<LvsInfo, SpdkData<Resource>, SpdkData<Snapshot>>
{
    private static final int TOLERANCE_FACTOR = 3;
    // FIXME: FORMAT should be private, only made public for LayeredSnapshotHelper
    public static final String FORMAT_RSC_TO_SPDK_ID = "%s%s_%05d";
    public static final String FORMAT_SNAP_TO_SPDK_ID = FORMAT_RSC_TO_SPDK_ID + "_%s";
    private static final String SPDK_FORMAT_DEV_PATH = SPDK_PATH_PREFIX + "%s/%s";

    private static final String DFLT_LVCREATE_TYPE = "linear";
    protected final SpdkCommands<T> spdkCommands;

    protected AbsSpdkProvider(
        ErrorReporter errorReporter,
        ExtCmdFactoryStlt extCmdFactory,
        AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider,
        String subTypeDescr,
        DeviceProviderKind subTypeKind,
        SnapshotShippingService snapShipMrgRef,
        StltExtToolsChecker extToolsCheckerRef,
        SpdkCommands<T> spdkCommandsRef,
        CloneService cloneServiceRef,
        BackupShippingMgr backupShipMgrRef
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
            subTypeKind,
            snapShipMrgRef,
            extToolsCheckerRef,
            cloneServiceRef,
            backupShipMgrRef
        );
        spdkCommands = spdkCommandsRef;
        isDevPathExpectedToBeNull = true;
    }

    @Override
    protected void updateStates(List<SpdkData<Resource>> vlmDataList, List<SpdkData<Snapshot>> snapshots)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        final Map<String, Long> extentSizes = SpdkUtils.getExtentSize(
            spdkCommands,
            getAffectedVolumeGroups(vlmDataList, snapshots)
        );

        List<SpdkData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataList);
        combinedList.addAll(snapshots);

        for (SpdkData<?> vlmData : combinedList)
        {

            final LvsInfo info = infoListCache.get(getFullQualifiedIdentifier(vlmData));
            updateInfo(vlmData, info);

            if (info != null)
            {
                final long expectedSize = vlmData.getExpectedSize();
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

                        final long toleratedSize = expectedSize + extentSizes.get(info.volumeGroup) * TOLERANCE_FACTOR;
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
    }

    private String getFullQualifiedIdentifier(SpdkData<?> vlmDataRef)
    {
        return vlmDataRef.getVolumeGroup() +
            File.separator +
            asIdentifierRaw(vlmDataRef);
    }

    @SuppressWarnings("unchecked")
    protected String asIdentifierRaw(SpdkData<?> vlmData)
    {
        String identifier;
        if (vlmData.getVolume() instanceof Volume)
        {
            identifier = asLvIdentifier((SpdkData<Resource>) vlmData);
        }
        else
        {
            identifier = asSnapLvIdentifier((SpdkData<Snapshot>) vlmData);
        }
        return identifier;
    }

    protected void updateInfo(SpdkData<?> vlmData, LvsInfo info)
        throws DatabaseException, AccessDeniedException, StorageException
    {
        vlmData.setIdentifier(asIdentifierRaw(vlmData));
        if (info == null)
        {
            vlmData.setExists(false);
            vlmData.setVolumeGroup(extractVolumeGroup(vlmData));
            vlmData.setDevicePath(null);
            // vlmData.setIdentifier(null);
            vlmData.setAllocatedSize(-1);
            // vlmData.setUsableSize(-1);
        }
        else
        {
            vlmData.setExists(true);
            vlmData.setVolumeGroup(info.volumeGroup);
            vlmData.setDevicePath(null); // if devicePath != null DevHandler will ask udev to get symlinks
            // pointing to that devPath which will fail
            vlmData.setSpdkPath(info.path);
            vlmData.setIdentifier(info.identifier);

            long size = SizeConv.convert(info.size, SizeUnit.UNIT_B, SizeUnit.UNIT_KiB);
            vlmData.setAllocatedSize(size);
            vlmData.setUsableSize(size);
        }
    }

    protected String extractVolumeGroup(SpdkData<?> vlmData)
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
    protected void createLvImpl(SpdkData<Resource> vlmData)
        throws StorageException, AccessDeniedException
    {
        spdkCommands.createFat(
            vlmData.getVolumeGroup(),
            asLvIdentifier(vlmData),
            vlmData.getExpectedSize(),
            "--type=" + getLvCreateType(vlmData)
        );
    }

    protected String getLvCreateType(SpdkData<Resource> vlmDataRef)
    {
        String type;
        try
        {
            Volume vlm = (Volume) vlmDataRef.getVolume();
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
    protected void resizeLvImpl(SpdkData<Resource> vlmData)
        throws StorageException, AccessDeniedException
    {
        spdkCommands.resize(
            vlmData.getVolumeGroup(),
            asLvIdentifier(vlmData),
            vlmData.getExpectedSize()
        );
    }

    @Override
    protected void deleteLvImpl(SpdkData<Resource> vlmData, String oldSpdkId)
        throws StorageException, DatabaseException, AccessDeniedException
    {
        vlmData.setExists(false);

        // SPDK by default wipes a lvol bdev during deletion, unless this option was disabled in lvol store
        try
        {
            spdkCommands.delete(
                vlmData.getVolumeGroup(),
                oldSpdkId
            );
        }
        catch (StorageException exc)
        {
            errorReporter.reportError(exc);
        }
    }

    @Override
    protected void deactivateLvImpl(SpdkData<Resource> vlmDataRef, String lvIdRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // noop, not supported
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        Map<String, Long> freeSizes;
        try
        {
            freeSizes = SpdkUtils.getVgFreeSize(spdkCommands, changedStoragePoolStrings);
            for (String storPool : changedStoragePoolStrings)
            {
                if (!freeSizes.containsKey(storPool))
                {
                    freeSizes.put(storPool, SIZE_OF_NOT_FOUND_STOR_POOL);
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return freeSizes;
    }

    @Override
    protected Map<String, LvsInfo> getInfoListImpl(
        List<SpdkData<Resource>> vlmDataList,
        List<SpdkData<Snapshot>> snapVlms
    )
        throws StorageException, AccessDeniedException
    {
        return SpdkUtils.getLvsInfo(
            spdkCommands,
            getAffectedVolumeGroups(vlmDataList, snapVlms)
        );
    }

    @Override
    public String getDevicePath(String storageName, String lvId)
    {
        return String.format(SPDK_FORMAT_DEV_PATH, storageName, lvId);
    }

    @Override
    protected String asLvIdentifier(
        StorPoolName ignoredSpName,
        ResourceName resourceName,
        String rscNameSuffix,
        VolumeNumber volumeNumber
    )
    {
        return String.format(
            FORMAT_RSC_TO_SPDK_ID,
            resourceName.displayValue,
            rscNameSuffix,
            volumeNumber.value
        );
    }

    @Override
    protected String asSnapLvIdentifierRaw(
        String ignoredSpName,
        String rscNameRef,
        String rscNameSuffixRef,
        String snapNameRef,
        int vlmNrRef
    )
    {
        return String.format(
            FORMAT_SNAP_TO_SPDK_ID,
            rscNameRef,
            rscNameSuffixRef,
            vlmNrRef,
            snapNameRef
        );
    }

    @Override
    protected String getStorageName(StorPool storPoolRef)
    {
        return getVolumeGroup(storPoolRef);
    }

    protected String getVolumeGroup(StorPoolInfo storPool)
    {
        String volumeGroup;
        try
        {
            volumeGroup = DeviceLayerUtils.getNamespaceStorDriver(
                storPool.getReadOnlyProps(storDriverAccCtx)
            )
                .getProp(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY).split("/")[0];
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
    public SpaceInfo getSpaceInfo(StorPoolInfo storPool) throws StorageException, AccessDeniedException
    {
        String vg = getVolumeGroup(storPool);
        if (vg == null)
        {
            throw new StorageException("Unset volume group for " + storPool);
        }
        Long capacity = SpdkUtils.getVgTotalSize(
            spdkCommands,
            Collections.singleton(vg)
        ).get(vg);
        Long freespace = SpdkUtils.getVgFreeSize(
            spdkCommands,
            Collections.singleton(vg)
        ).get(vg);

        return SpaceInfo.buildOrThrowOnError(capacity, freespace, storPool);
    }

    @Override
    protected boolean waitForSnapshotDevice()
    {
        return true;
    }

    @Override
    public @Nullable LocalPropsChangePojo update(StorPool storPoolRef)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        // TODO: we need to implement a check for pmem here. something like LvmProvider.update does
        return null;
    }

    @Override
    public @Nullable LocalPropsChangePojo checkConfig(StorPoolInfo storPool)
        throws StorageException, AccessDeniedException
    {
        LocalPropsChangePojo ret = new LocalPropsChangePojo();

        ReadOnlyProps props = DeviceLayerUtils.getNamespaceStorDriver(
            storPool.getReadOnlyProps(storDriverAccCtx)
        );
        SpdkConfigReader.checkVolumeGroupEntry(spdkCommands, props);
        SpdkConfigReader.checkToleranceFactor(props);

        checkExtentSize(storPool, ret);

        return ret;
    }

    protected void checkExtentSize(StorPoolInfo storPool, LocalPropsChangePojo ret)
        throws StorageException, ImplementationError, AccessDeniedException
    {
        String vlmGrp = getVolumeGroup(storPool);
        final Map<String, Long> extentSizes = SpdkUtils.getExtentSize(
            spdkCommands,
            Collections.singleton(vlmGrp)
        );
        Long extentSizeInKib = extentSizes.get(vlmGrp);
        markAllocGranAsChangedIfNeeded(extentSizeInKib, storPool, ret);
    }

    @Override
    protected long getAllocatedSize(SpdkData<Resource> vlmDataRef) throws StorageException
    {
        try
        {
            return SpdkUtils.getBlockSizeByName(
                spdkCommands,
                vlmDataRef.getSpdkPath().split(SPDK_PATH_PREFIX)[1]
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Set<String> getAffectedVolumeGroups(
        Collection<SpdkData<Resource>> vlmDataList,
        Collection<SpdkData<Snapshot>> snapVlms
    )
    {
        ArrayList<SpdkData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataList);
        combinedList.addAll(snapVlms);

        Set<String> volumeGroups = new HashSet<>();
        for (SpdkData<?> vlmData : combinedList)
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
        return volumeGroups;
    }

    /*
     * Snapshots
     */
    @Override
    protected boolean snapshotExists(SpdkData<Snapshot> snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        String identifier = getFullQualifiedIdentifier(snapVlmRef);

        return infoListCache.get(identifier) != null;
    }

    @Override
    protected void createSnapshot(SpdkData<Resource> vlmData, SpdkData<Snapshot> snapVlm)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        spdkCommands.createSnapshot(
            getFullQualifiedIdentifier(vlmData),
            asSnapLvIdentifier(snapVlm)
        );
    }

    @Override
    protected void restoreSnapshot(String sourceLvIdRef, String sourceSnapNameRef, SpdkData<Resource> vlmDataRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // DO NOT use asSnapLvIdentifier here as sourceLvIdRef already contains the rscName + rscNameSuffix + vlmNr.
        // just appending the snapname should be sufficient
        String fullQualifiedSnapName = vlmDataRef.getVolumeGroup() + "/" + sourceLvIdRef + "_" + sourceSnapNameRef;

        spdkCommands.restoreSnapshot(
            fullQualifiedSnapName,
            asLvIdentifier(vlmDataRef)
        );
        spdkCommands.decoupleParent(getFullQualifiedIdentifier(vlmDataRef));
    }

    @Override
    protected void rollbackImpl(SpdkData<Resource> vlmDataRef, String rollbackTargetSnapshotNameRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        String vlmGrp = vlmDataRef.getVolumeGroup();

        String fullQualSnapName = vlmGrp + "/" + asSnapLvIdentifierRaw(
            vlmDataRef.getStorPool().getName().displayValue,
            vlmDataRef.getRscLayerObject().getResourceName().displayValue,
            vlmDataRef.getRscLayerObject().getResourceNameSuffix(),
            rollbackTargetSnapshotNameRef,
            vlmDataRef.getVlmNr().value
        );
        String vlmLvId = asLvIdentifier(vlmDataRef);
        String rollbackVlmLvId = vlmLvId + "_rollback";
        String fullQualVlmName = vlmDataRef.getVolumeGroup() + "/" + vlmLvId;

        spdkCommands.clone(fullQualSnapName, rollbackVlmLvId);

        final String nqn = NvmeUtils.STANDARD_NVME_SUBSYSTEM_PREFIX +
            vlmDataRef.getRscLayerObject().getSuffixedResourceName();
        spdkCommands.nvmfSubsystemRemoveNamespace(nqn, 1);

        spdkCommands.delete(vlmGrp, vlmLvId);
        spdkCommands.rename(vlmGrp, rollbackVlmLvId, vlmLvId);
        spdkCommands.nvmfSubsystemAddNs(nqn, fullQualVlmName);

        spdkCommands.decoupleParent(fullQualVlmName);
    }

    @Override
    protected void deleteSnapshotImpl(SpdkData<Snapshot> snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // no difference between snapshot and volume on SPDK level
        spdkCommands.delete(snapVlmRef.getVolumeGroup(), asSnapLvIdentifier(snapVlmRef));
        snapVlmRef.setExists(false);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected long getExtentSize(AbsStorageVlmData<?> vlmDataRef) throws StorageException, AccessDeniedException
    {
        Collection<SpdkData<Resource>> vlmDataList;
        Collection<SpdkData<Snapshot>> snapVlms;
        if (vlmDataRef.getVolume() instanceof Volume)
        {
            vlmDataList = Collections.singleton((SpdkData<Resource>) vlmDataRef);
            snapVlms = Collections.emptyList();
        }
        else
        {
            vlmDataList = Collections.emptyList();
            snapVlms = Collections.singleton((SpdkData<Snapshot>) vlmDataRef);
        }
        Set<String> affectedVolumeGroups = getAffectedVolumeGroups(
            vlmDataList,
            snapVlms
        );
        if (affectedVolumeGroups.size() != 1)
        {
            throw new StorageException("Could not find volume group for volume data: " + vlmDataRef);
        }
        String vlmGrp = affectedVolumeGroups.iterator().next();
        final Map<String, Long> extentSizes = SpdkUtils.getExtentSize(
            spdkCommands,
            affectedVolumeGroups
        );

        return extentSizes.get(vlmGrp);
    }

    @Override
    protected void setDevicePath(SpdkData<Resource> vlmData, String devPath) throws DatabaseException
    {
        vlmData.setSpdkPath(devPath);
    }

    @Override
    protected void setAllocatedSize(SpdkData<Resource> vlmData, long size) throws DatabaseException
    {
        vlmData.setAllocatedSize(size);
    }

    @Override
    protected void setUsableSize(SpdkData<Resource> vlmData, long size) throws DatabaseException
    {
        vlmData.setUsableSize(size);
    }

    @Override
    protected void setExpectedUsableSize(SpdkData<Resource> vlmData, long size)
    {
        vlmData.setExpectedSize(size);
    }

    @Override
    protected String getStorageName(SpdkData<Resource> vlmDataRef) throws DatabaseException
    {
        return vlmDataRef.getVolumeGroup();
    }

    public SpdkCommands<?> getSpdkCommands()
    {
        return spdkCommands;
    }

    @Override
    public Map<ReadOnlyVlmProviderInfo, Long> fetchAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException, AccessDeniedException
    {
        return fetchOrigAllocatedSizes(vlmDataListRef);
    }
}
