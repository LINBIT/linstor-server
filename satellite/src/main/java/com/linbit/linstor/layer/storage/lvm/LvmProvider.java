package com.linbit.linstor.layer.storage.lvm;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.DeviceLayerUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.layer.storage.utils.LsBlkUtils;
import com.linbit.linstor.layer.storage.utils.MkfsUtils;
import com.linbit.linstor.layer.storage.utils.PmemUtils;
import com.linbit.linstor.layer.storage.utils.StorageConfigReader;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
public class LvmProvider extends AbsStorageProvider<LvsInfo, LvmData<Resource>, LvmData<Snapshot>>
{
    private static final int TOLERANCE_FACTOR = 3;
    // FIXME: FORMAT should be private, only made public for LayeredSnapshotHelper
    public static final String FORMAT_RSC_TO_LVM_ID = "%s%s_%05d";
    public static final String FORMAT_SNAP_TO_LVM_ID = FORMAT_RSC_TO_LVM_ID + "_%s";
    private static final String FORMAT_LVM_ID_WIPE_IN_PROGRESS = "%s-linstor_wiping_in_progress-%d";
    private static final String FORMAT_DEV_PATH = "/dev/%s/%s";

    private static final String DFLT_LVCREATE_TYPE = "linear";

    private static final AtomicLong DELETED_ID = new AtomicLong(0);

    protected LvmProvider(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider,
        String subTypeDescr,
        DeviceProviderKind subTypeKind,
        SnapshotShippingService snapShipMrgRef
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
            snapShipMrgRef
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
        Provider<TransactionMgr> transMgrProvider,
        SnapshotShippingService snapShipMrgRef
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
            DeviceProviderKind.LVM,
            snapShipMrgRef
        );
    }

    @Override
    protected void updateStates(List<LvmData<Resource>> vlmDataList, List<LvmData<Snapshot>> snapVlmDataList)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        final Map<String, Long> extentSizes = LvmUtils.getExtentSize(
            extCmdFactory,
            getAffectedVolumeGroups(vlmDataList, snapVlmDataList)
        );

        List<LvmData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataList);
        combinedList.addAll(snapVlmDataList);

        for (LvmData<?> vlmData : combinedList)
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
    }

    protected String getFullQualifiedIdentifier(LvmData<?> vlmDataRef)
    {
        return vlmDataRef.getVolumeGroup() +
            File.separator +
            asIdentifierRaw(vlmDataRef);
    }

    @SuppressWarnings("unchecked")
    protected String asIdentifierRaw(LvmData<?> vlmData)
    {
        String identifier;
        if (vlmData.getVolume() instanceof Volume)
        {
            identifier = asLvIdentifier((LvmData<Resource>) vlmData);
        }
        else
        {
            identifier = asSnapLvIdentifier((LvmData<Snapshot>) vlmData);
        }
        return identifier;
    }

    /*
     * Expected to be overridden (extended) by LvmThinProvider
     */
    @SuppressWarnings({"unchecked", "unused"})
    protected void updateInfo(LvmData<?> vlmDataRef, LvsInfo info)
        throws DatabaseException, AccessDeniedException, StorageException
    {
        if (vlmDataRef.getVolume() instanceof Volume)
        {
            vlmDataRef.setIdentifier(asLvIdentifier((LvmData<Resource>) vlmDataRef));
        }
        else
        {
            vlmDataRef.setIdentifier(asSnapLvIdentifier((LvmData<Snapshot>) vlmDataRef));
        }
        if (info == null)
        {
            vlmDataRef.setExists(false);
            vlmDataRef.setVolumeGroup(extractVolumeGroup(vlmDataRef));
            vlmDataRef.setDevicePath(null);
            vlmDataRef.setAllocatedSize(-1);
            // vlmData.setUsableSize(-1);
            vlmDataRef.setDevicePath(null);
            vlmDataRef.setAttributes(null);
        }
        else
        {
            vlmDataRef.setExists(true);
            vlmDataRef.setVolumeGroup(info.volumeGroup);
            vlmDataRef.setDevicePath(info.path);
            vlmDataRef.setIdentifier(info.identifier);
            vlmDataRef.setAllocatedSize(info.size);
            vlmDataRef.setUsableSize(info.size);
            vlmDataRef.setAttributes(info.attributes);

            if (!info.attributes.contains("a"))
            {
                LvmUtils.execWithRetry(
                    extCmdFactory,
                    Collections.singleton(vlmDataRef.getVolumeGroup()),
                    config -> LvmCommands.activateVolume(
                        extCmdFactory.create(),
                        vlmDataRef.getVolumeGroup(),
                        vlmDataRef.getIdentifier(),
                        config
                    )
                );
            }
        }
    }

    protected String extractVolumeGroup(LvmData<?> vlmData)
    {
        return getVolumeGroup(vlmData.getStorPool());
    }

    @Override
    protected void createLvImpl(LvmData<Resource> vlmData)
        throws StorageException, AccessDeniedException
    {
        List<String> additionalOptions = MkfsUtils.shellSplit(getLvcreateOptions(vlmData));
        String[] additionalOptionsArr = new String[additionalOptions.size()];
        additionalOptions.toArray(additionalOptionsArr);

        if (additionalOptions.contains("--config"))
        {
            // no retry, use only users '--config' settings
            LvmCommands.createFat(
                extCmdFactory.create(),
                vlmData.getVolumeGroup(),
                asLvIdentifier(vlmData),
                vlmData.getExepectedSize(),
                null, // config is contained in additionalOptions
                additionalOptionsArr
            );
        }
        else
        {
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(vlmData.getVolumeGroup()),
                config -> LvmCommands.createFat(
                    extCmdFactory.create(),
                    vlmData.getVolumeGroup(),
                    asLvIdentifier(vlmData),
                    vlmData.getExepectedSize(),
                    config,
                    additionalOptionsArr
                )
            );
        }
    }

    protected String getLvCreateType(LvmData<Resource> vlmDataRef)
    {
        String type;
        try
        {
            type = getPrioProps(vlmDataRef)
                .getProp(
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

    protected PriorityProps getPrioProps(LvmData<Resource> vlmDataRef) throws AccessDeniedException
    {
        Volume vlm = (Volume) vlmDataRef.getVolume();
        Resource rsc = vlm.getAbsResource();
        ResourceDefinition rscDfn = vlm.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        return new PriorityProps(
            vlm.getProps(storDriverAccCtx),
            rsc.getProps(storDriverAccCtx),
            vlmDataRef.getStorPool().getProps(storDriverAccCtx),
            rsc.getNode().getProps(storDriverAccCtx),
            vlmDfn.getProps(storDriverAccCtx),
            rscDfn.getProps(storDriverAccCtx),
            rscGrp.getVolumeGroupProps(storDriverAccCtx, vlmDfn.getVolumeNumber()),
            rscGrp.getProps(storDriverAccCtx),
            stltConfigAccessor.getReadonlyProps()
        );
    }

    protected String getLvcreateOptions(LvmData<Resource> vlmDataRef)
    {
        String options;
        try
        {
            options = getPrioProps(vlmDataRef).getProp(
                ApiConsts.KEY_STOR_POOL_LVCREATE_OPTIONS,
                ApiConsts.NAMESPC_STORAGE_DRIVER,
                ""
            );
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return options;
    }

    @Override
    protected void resizeLvImpl(LvmData<Resource> vlmData)
        throws StorageException, AccessDeniedException
    {
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(vlmData.getVolumeGroup()),
            config -> LvmCommands.resize(
                extCmdFactory.create(),
                vlmData.getVolumeGroup(),
                asLvIdentifier(vlmData),
                vlmData.getExepectedSize(),
                config
            )
        );
    }

    @Override
    protected void deleteLvImpl(LvmData<Resource> vlmData, String oldLvmId)
        throws StorageException, DatabaseException, AccessDeniedException
    {
        String devicePath = vlmData.getDevicePath();
        String volumeGroup = vlmData.getVolumeGroup();

        if (true)
        {
            wipeHandler.quickWipe(devicePath);
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(vlmData.getVolumeGroup()),
                config -> LvmCommands.delete(
                    extCmdFactory.create(),
                    volumeGroup,
                    oldLvmId,
                    config
                )
            );
            vlmData.setExists(false);
        }
        else
        {
            // TODO use this path once async wiping is implemented

            // devicePath is the "current" devicePath. as we will rename it right now
            // we will have to adjust the devicePath
            int lastIndexOf = devicePath.lastIndexOf(oldLvmId);

            // just make sure to not colide with any other ongoing wipe-lv-name
            String newLvmId = String.format(
                FORMAT_LVM_ID_WIPE_IN_PROGRESS,
                asLvIdentifier(vlmData),
                DELETED_ID.incrementAndGet()
            );
            devicePath = devicePath.substring(0, lastIndexOf) + newLvmId;

            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(vlmData.getVolumeGroup()),
                config -> LvmCommands.rename(
                    extCmdFactory.create(),
                    volumeGroup,
                    oldLvmId,
                    newLvmId,
                    config
                )
            );

            vlmData.setExists(false);

            wipeHandler.asyncWipe(
                devicePath,
                ignored ->
                {
                    LvmUtils.execWithRetry(
                        extCmdFactory,
                        Collections.singleton(vlmData.getVolumeGroup()),
                        config -> LvmCommands.delete(
                            extCmdFactory.create(),
                            volumeGroup,
                            newLvmId,
                            config
                        )
                    );
                }
            );
        }
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        Map<String, Long> freeSizes = LvmUtils.getVgFreeSize(extCmdFactory, changedStoragePoolStrings);
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
    protected Map<String, LvsInfo> getInfoListImpl(
        List<LvmData<Resource>> vlmDataList,
        List<LvmData<Snapshot>> snapVlms
    )
        throws StorageException, AccessDeniedException
    {
        if (hasSharedVolumeGroups(vlmDataList, snapVlms))
        {
            LvmCommands.lvscan(extCmdFactory.create());
        }

        return LvmUtils.getLvsInfo(
            extCmdFactory,
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
    protected String asSnapLvIdentifierRaw(String rscNameRef, String rscNameSuffixRef, String snapNameRef, int vlmNrRef)
    {
        return String.format(
            FORMAT_SNAP_TO_LVM_ID,
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

    protected String getVolumeGroup(StorPool storPool)
    {
        String volumeGroup;
        try
        {
            volumeGroup = DeviceLayerUtils.getNamespaceStorDriver(
                    storPool.getProps(storDriverAccCtx)
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
        return true; // LVM driver should call dmstats commands
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String vg = getVolumeGroup(storPool);
        if (vg == null)
        {
            throw new StorageException("Unset volume group for " + storPool);
        }
        Long capacity = LvmUtils.getVgTotalSize(
            extCmdFactory,
            Collections.singleton(vg)
        ).get(vg);
        Long freespace = LvmUtils.getVgFreeSize(
            extCmdFactory,
            Collections.singleton(vg)
        ).get(vg);
        return new SpaceInfo(capacity, freespace);
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
        StorageConfigReader.checkVolumeGroupEntry(extCmdFactory, props);
        StorageConfigReader.checkToleranceFactor(props);
    }

    @Override
    public void update(StorPool storPoolRef) throws AccessDeniedException, DatabaseException, StorageException
    {
        List<String> pvs = LvmUtils.getPhysicalVolumes(extCmdFactory, getVolumeGroup(storPoolRef));
        if (PmemUtils.supportsDax(extCmdFactory.create(), pvs))
        {
            storPoolRef.setPmem(true);
        }
        storPoolRef.setVDO(LsBlkUtils.parentIsVDO(extCmdFactory.create(), pvs));
    }

    private Set<String> getAffectedVolumeGroups(
        Collection<LvmData<Resource>> vlmDataList,
        Collection<LvmData<Snapshot>> snapVlms
    )
    {
        ArrayList<LvmData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataList);
        combinedList.addAll(snapVlms);

        Set<String> volumeGroups = new HashSet<>();
        for (LvmData<?> vlmData : combinedList)
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

    private boolean hasSharedVolumeGroups(
        Collection<LvmData<Resource>> vlmDataList,
        Collection<LvmData<Snapshot>> snapVlms
    )
    {
        boolean ret = false;
        ArrayList<LvmData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataList);
        combinedList.addAll(snapVlms);

        for (LvmData<?> vlmData : combinedList)
        {
            StorPool storPool = vlmData.getStorPool();
            if (storPool.getSharedStorPoolName().isShared())
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    @Override
    protected boolean waitForSnapshotDevice()
    {
        return true;
    }

    @Override
    protected void setDevicePath(LvmData<Resource> vlmData, String devPath) throws DatabaseException
    {
        vlmData.setDevicePath(devPath);
    }

    @Override
    protected void setAllocatedSize(LvmData<Resource> vlmData, long size) throws DatabaseException
    {
        vlmData.setAllocatedSize(size);
    }

    @Override
    protected void setUsableSize(LvmData<Resource> vlmData, long size) throws DatabaseException
    {
        vlmData.setUsableSize(size);
    }

    @Override
    protected void setExpectedUsableSize(LvmData<Resource> vlmData, long size)
    {
        vlmData.setExepectedSize(size);
    }

    @Override
    protected String getStorageName(LvmData<Resource> vlmDataRef) throws DatabaseException
    {
        return vlmDataRef.getVolumeGroup();
    }
}
