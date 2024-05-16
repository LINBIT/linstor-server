package com.linbit.linstor.layer.storage.zfs;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.DeviceLayerUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.storage.utils.PmemUtils;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsCommands;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsCommands.ZfsVolumeType;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsUtils;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsUtils.ZfsInfo;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.utils.MkfsUtils;
import com.linbit.linstor.storage.utils.ZfsPropsUtils;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

@Singleton
public class ZfsProvider extends AbsStorageProvider<ZfsInfo, ZfsData<Resource>, ZfsData<Snapshot>>
{
    // FIXME: FORMAT should be private, only made public for LayeredSnapshotHelper
    public static final String FORMAT_RSC_TO_ZFS_ID = "%s%s_%05d";
    public static final String FORMAT_SNAP_TO_ZFS_ID = FORMAT_RSC_TO_ZFS_ID + "@%s";
    private static final String FORMAT_ZFS_DEV_PATH = "/dev/zvol/%s/%s";
    private static final int TOLERANCE_FACTOR = 3;

    private static final long ZFS_DFLT_EXTENT_SIZE_IN_KIB = 8L;
    private static final ExtToolsInfo.Version VERSION_2_0_0 = new ExtToolsInfo.Version(2, 0, 0);

    private  Map<StorPool, Long> extentSizes = new TreeMap<>();

    protected ZfsProvider(
        ErrorReporter errorReporter,
        ExtCmdFactoryStlt extCmdFactory,
        AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider,
        String subTypeDescr,
        DeviceProviderKind kind,
        SnapshotShippingService snapShipMrgRef,
        StltExtToolsChecker extToolsCheckerRef,
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
            kind,
            snapShipMrgRef,
            extToolsCheckerRef,
            cloneServiceRef,
            backupShipMgrRef
        );
    }

    @Inject
    public ZfsProvider(
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
            "ZFS",
            DeviceProviderKind.ZFS,
            snapShipMrgRef,
            extToolsCheckerRef,
            cloneServiceRef,
            backupShipMgrRef
        );
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.ZFS;
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
        List<ZfsData<Resource>> vlmDataListRef,
        List<ZfsData<Snapshot>> snapVlmsRef
    )
        throws StorageException
    {
        return ZfsUtils.getZfsList(
            extCmdFactory.create(),
            getDataSets(vlmDataListRef, snapVlmsRef),
            kind
        );
    }

    private Set<String> getDataSets(List<ZfsData<Resource>> vlmDataListRef, List<ZfsData<Snapshot>> snapVlmsRef)
    {
        List<ZfsData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataListRef);
        combinedList.addAll(snapVlmsRef);

        Set<String> dataSets = new HashSet<>();
        for (ZfsData<?> data : combinedList)
        {
            dataSets.add(getZPool(data.getStorPool()));
        }
        return dataSets;
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
            FORMAT_SNAP_TO_ZFS_ID,
            rscNameRef,
            rscNameSuffixRef,
            vlmNrRef,
            snapNameRef
        );
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
            FORMAT_RSC_TO_ZFS_ID,
            resourceName.displayValue,
            rscNameSuffix,
            volumeNumber.value
        );
    }

    @SuppressWarnings("unchecked")
    private String asIdentifierRaw(ZfsData<?> zfsData)
    {
        AbsVolume<?> volume = zfsData.getVolume();
        String identifier;
        if (volume instanceof Volume)
        {
            identifier = asLvIdentifier((ZfsData<Resource>) zfsData);
        }
        else
        {
            identifier = asSnapLvIdentifier((ZfsData<Snapshot>) zfsData);
        }
        return identifier;
    }

    private String asFullQualifiedLvIdentifier(ZfsData<?> zfsData)
        throws AccessDeniedException
    {
        return getZPool(zfsData.getStorPool()) + File.separator +
            asIdentifierRaw(zfsData);
    }


    @Override
    protected void createLvImpl(ZfsData<Resource> vlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.create(
            extCmdFactory.create(),
            vlmData.getZPool(),
            asLvIdentifier(vlmData),
            vlmData.getExpectedSize(),
            false,
            getZfscreateOptions(vlmData)
        );
    }

    protected String[] getZfscreateOptions(ZfsData<Resource> vlmDataRef)
    {
        return getProp(vlmDataRef, ApiConsts.NAMESPC_STORAGE_DRIVER, ApiConsts.KEY_STOR_POOL_ZFS_CREATE_OPTIONS, "");
    }

    protected String[] getZfsSnapshotOptions(ZfsData<Resource> vlmDataRef)
    {
        return getProp(vlmDataRef, ApiConsts.NAMESPC_STORAGE_DRIVER, ApiConsts.KEY_STOR_POOL_ZFS_SNAPSHOT_OPTIONS, "");
    }

    protected String[] getProp(ZfsData<Resource> vlmDataRef, String namespace, String key, String dfltValue)
    {
        String[] additionalOptionsArr;

        try
        {
            String options = getPrioProps(vlmDataRef).getProp(
                key,
                namespace,
                dfltValue
            );
            List<String> additionalOptions = MkfsUtils.shellSplit(options);
            additionalOptionsArr = new String[additionalOptions.size()];
            additionalOptions.toArray(additionalOptionsArr);
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return additionalOptionsArr;
    }

    @Override
    protected long getExtentSize(AbsStorageVlmData<?> vlmDataRef) throws StorageException
    {
        try
        {
            return ZfsPropsUtils.extractZfsVolBlockSizePrivileged(
                (ZfsData<?>) vlmDataRef,
                storDriverAccCtx,
                stltConfigAccessor.getReadonlyProps()
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * Brief summary of ZFS versions and default <code>volBlockSize</code>:
     * <ul>
     * <li>Before version 0.8.0, ZFS had no '<code>zfs version</code>' or '<code>zfs --version</code>'. Default
     * <code>volBlockSize</code> is 8k.</li>
     * <li>Before version 2.0.0, ZFS had no '<code>zfs create ... --dry-run --verbose --parseable</code>' options.
     * Default <code>volBlockSize</code> is still 8k.</li>
     * <li>Since ZFS version 2.2.0 the default <code>volBlockSize</code> is increased to 16k (openzfs git hash
     * <code>72f0521</code>).</li>
     * </ul>
     * This means, that LINSTOR can return <code>8k</code> for versions pre 2.0.0, and for versions since 2.0.0 "query"
     * the current <code>volBlockSize</code> using
     * '<code>zfs create zpool/dummy-volume -V 1B --dry-run --parseable</code>'. Here is an example result of this
     * command:
     *
     * <pre>
    # zfs create -n -P -V 1B scratch-zfs/dummy
    create  scratch-zfs/dummy
    property    volsize 8192
    property    refreservation  1843200
    #
     * </pre>
     */
    protected long getExtentSize(StorPool storPoolRef) throws StorageException
    {
        long ret = ZFS_DFLT_EXTENT_SIZE_IN_KIB;
        Map<ExtTools, ExtToolsInfo> externalTools = extToolsChecker.getExternalTools(false);
        @Nullable ExtToolsInfo zfsUtilsInfo = externalTools.get(ExtTools.ZFS_UTILS);
        @Nullable ExtToolsInfo zfsKmodInfo = externalTools.get(ExtTools.ZFS_KMOD);

        if (zfsUtilsInfo != null && zfsKmodInfo != null && zfsUtilsInfo.isSupported() && zfsKmodInfo.isSupported())
        {
            ExtToolsInfo.Version zfsUtilsVersion = zfsUtilsInfo.getVersion();
            ExtToolsInfo.Version zfsKmodVersion = zfsKmodInfo.getVersion();

            if (zfsUtilsVersion.getMajor() != null && zfsUtilsVersion.greaterOrEqual(VERSION_2_0_0) &&
                zfsKmodVersion.getMajor() != null && zfsKmodVersion.greaterOrEqual(VERSION_2_0_0))
            {
                ret = ZfsUtils.getZfsExtentSize(extCmdFactory.create(), getZPool(storPoolRef));
            }
        }
        return ret;
    }

    @Override
    protected void resizeLvImpl(ZfsData<Resource> vlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.resize(
            extCmdFactory.create(),
            vlmData.getZPool(),
            asLvIdentifier(vlmData),
            vlmData.getExpectedSize()
        );
    }

    @Override
    protected void deleteLvImpl(ZfsData<Resource> vlmData, String lvId)
        throws StorageException, DatabaseException
    {
        ZfsCommands.delete(
            extCmdFactory.create(),
            vlmData.getZPool(),
            lvId,
            ZfsVolumeType.VOLUME
        );
        vlmData.setExists(false);

        // This block will delete the clone snapshot on the base resource if `zfs clone` was used
        // if deleting fails we should just ignore it as it doesn't have any impact on the
        // deletion of the child resource
        try
        {
            String useZfsClone = vlmData.getRscLayerObject().getAbsResource()
                .getResourceDefinition().getProps(storDriverAccCtx).getProp(InternalApiConsts.KEY_USE_ZFS_CLONE);
            if (useZfsClone != null)
            {
                String clonedFromRsc = vlmData.getRscLayerObject().getAbsResource()
                    .getResourceDefinition().getProps(storDriverAccCtx).getProp(InternalApiConsts.KEY_CLONED_FROM);
                String srcFullSnapshotName = asSnapLvIdentifierRaw(
                    vlmData.getStorPool().getName().displayValue,
                    clonedFromRsc,
                    vlmData.getRscLayerObject().getResourceNameSuffix(),
                    getCloneSnapshotName(vlmData),
                    vlmData.getVlmNr().value);
                ZfsCommands.delete(
                    extCmdFactory.create(),
                    getZPool(vlmData.getStorPool()),
                    srcFullSnapshotName,
                    ZfsVolumeType.SNAPSHOT
                );
            }
        }
        catch (AccessDeniedException | StorageException exc)
        {
            // just report that we couldn't delete, it isn't a fatal error,
            // and it is possible that a toggled disk doesn't have any base snapshot
            errorReporter.logWarning("Unable to remove base snapshot of resource %s",
                vlmData.getRscLayerObject().getResourceName());
        }
    }

    @Override
    protected void deactivateLvImpl(ZfsData<Resource> vlmDataRef, String lvIdRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // noop, not supported
    }

    @Override
    public boolean snapshotExists(ZfsData<Snapshot> snapVlm)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsInfo zfsInfo = infoListCache.get(asFullQualifiedLvIdentifier(snapVlm));
        return zfsInfo != null;
    }

    @Override
    protected void createSnapshot(ZfsData<Resource> vlmData, ZfsData<Snapshot> snapVlm)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        Snapshot snap = snapVlm.getVolume().getAbsResource();
        // snapshot will be created by "zfs receive" command
        if (!snap.getFlags().isSet(storDriverAccCtx, Snapshot.Flags.SHIPPING_TARGET))
        {
            ZfsCommands.createSnapshot(
                extCmdFactory.create(),
                vlmData.getZPool(),
                asLvIdentifier(vlmData),
                snapVlm.getVolume().getAbsResource().getSnapshotName().displayValue,
                getZfsSnapshotOptions(vlmData)
            );
        }
    }

    @Override
    protected void restoreSnapshot(String sourceLvId, String sourceSnapName, ZfsData<Resource> targetVlmData)
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
    protected void deleteSnapshotImpl(ZfsData<Snapshot> snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.delete(
            extCmdFactory.create(),
            getZPool(snapVlmRef.getStorPool()),
            asSnapLvIdentifier(snapVlmRef),
            ZfsVolumeType.SNAPSHOT
        );
    }

    @Override
    protected void rollbackImpl(ZfsData<Resource> vlmData, String rollbackTargetSnapshotName)
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
    public String getDevicePath(String zPool, String identifier)
    {
        return String.format(FORMAT_ZFS_DEV_PATH, zPool, identifier);
    }

    @Override
    protected String getStorageName(StorPool storPoolRef)
    {
        return getZPool(storPoolRef);
    }

    protected String getZPool(StorPool storPool)
    {
        String zPool;
        try
        {
            zPool = DeviceLayerUtils.getNamespaceStorDriver(
                storPool.getProps(storDriverAccCtx)
            ).getProp(StorageConstants.CONFIG_ZFS_POOL_KEY);
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return zPool;
    }

    @Override
    public @Nullable LocalPropsChangePojo checkConfig(StorPool storPool) throws StorageException, AccessDeniedException
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
        HashMap<String, ZfsInfo> zfsPoolMap = ZfsUtils.getThinZPoolsList(
            extCmdFactory.create(),
            Collections.singleton(zpoolName)
        );
        if (!zfsPoolMap.containsKey(zpoolName))
        {
            throw new StorageException("no zpool found with name '" + zpoolName + "'");
        }

        return null;
    }

    protected void checkExtentSize(StorPool storPool, LocalPropsChangePojo ret)
        throws ImplementationError, AccessDeniedException, StorageException
    {
        @Nullable String oldExtentSizeInKib = storPool.getProps(storDriverAccCtx)
            .getProp(
                InternalApiConsts.ALLOCATION_GRANULARITY,
                StorageConstants.NAMESPACE_INTERNAL
            );

        long currentExtentSize = getExtentSize(storPool);
        String currentExtentSizeStr = Long.toString(currentExtentSize);

        if (!Objects.equals(currentExtentSizeStr, oldExtentSizeInKib))
        {
            errorReporter.logTrace(
                "Found extent size for ZFS SP '%s': %dKiB",
                storPool.getName().displayValue,
                currentExtentSize
            );
            ret.changeStorPoolProp(
                storPool,
                StorageConstants.NAMESPACE_INTERNAL + "/" + InternalApiConsts.ALLOCATION_GRANULARITY,
                currentExtentSizeStr
            );
        }
    }

    @Override
    public @Nullable LocalPropsChangePojo update(StorPool storPoolRef)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        LocalPropsChangePojo ret = new LocalPropsChangePojo();
        List<String> pvs = ZfsUtils.getPhysicalVolumes(extCmdFactory.create(), getZpoolOnlyName(storPoolRef));
        if (PmemUtils.supportsDax(extCmdFactory.create(), pvs))
        {
            storPoolRef.setPmem(true);
        }
        checkExtentSize(storPoolRef, ret);

        return ret;
    }

    protected String getZpoolOnlyName(StorPool storPoolRef) throws StorageException
    {
        String zpoolName = getZPool(storPoolRef);
        if (zpoolName == null)
        {
            throw new StorageException("Unset zfs dataset for " + storPoolRef);
        }

        return ZfsUtils.getZPoolRootName(zpoolName);
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String zPool = getZPool(storPool);
        if (zPool == null)
        {
            throw new StorageException("Unset zfs dataset for " + storPool);
        }

        long capacity = ZfsUtils.getZPoolTotalSize(
            extCmdFactory.create(),
            Collections.singleton(zPool)
        ).get(zPool);
        long freeSpace = ZfsUtils.getZPoolFreeSize(
            extCmdFactory.create(),
            Collections.singleton(zPool)
        ).get(zPool);

        return SpaceInfo.buildOrThrowOnError(capacity, freeSpace, storPool);
    }

    @Override
    protected boolean updateDmStats()
    {
        return false;
    }

    @Override
    protected boolean waitForSnapshotDevice()
    {
        return false;
    }

    // @Override
    // protected void startReceiving(ZfsData<Resource> vlmDataRef, ZfsData<Snapshot> snapVlmDataRef)
    // throws AccessDeniedException, StorageException, DatabaseException
    // {
    // try
    // {
    // /*
    // * if this is the initial shipment, the actual volume must not exist.
    // */
    // Snapshot targetSnap = snapVlmDataRef.getRscLayerObject().getAbsResource();
    // SnapshotDefinition snapDfn = targetSnap.getSnapshotDefinition();
    // ResourceDefinition rscDfn = snapDfn.getResourceDefinition();
    //
    // String sourceNodeName = snapDfn.getProps(storDriverAccCtx)
    // .getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_SOURCE_NODE);
    // Resource targetRsc = rscDfn.getResource(storDriverAccCtx, targetSnap.getNodeName());
    // Resource sourceRsc = rscDfn.getResource(storDriverAccCtx, new NodeName(sourceNodeName));
    //
    // String prevShippingSnapName = targetRsc.getAbsResourceConnection(storDriverAccCtx, sourceRsc)
    // .getProps(storDriverAccCtx)
    // .getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_NAME_PREV);
    //
    // if (prevShippingSnapName == null)
    // {
    // deleteLvImpl(vlmDataRef, asLvIdentifier(vlmDataRef));
    // vlmDataRef.setInitialShipment(true);
    // }
    // }
    // catch (InvalidNameException exc)
    // {
    // throw new ImplementationError(exc);
    // }
    //
    // super.startReceiving(vlmDataRef, snapVlmDataRef);
    // }

    @Override
    protected String getSnapshotShippingReceivingCommandImpl(ZfsData<Snapshot> snapVlmDataRef)
        throws StorageException, AccessDeniedException
    {
        return "zfs receive -F " + getZPool(snapVlmDataRef.getStorPool()) + "/" +
            asSnapLvIdentifier(snapVlmDataRef);
    }

    @Override
    protected String getSnapshotShippingSendingCommandImpl(
        ZfsData<Snapshot> lastSnapVlmDataRef,
        ZfsData<Snapshot> curSnapVlmDataRef
    )
        throws StorageException, AccessDeniedException
    {
        StringBuilder sb = new StringBuilder("zfs send ");
        if (lastSnapVlmDataRef != null)
        {
            sb.append("-i ") // incremental
                .append(getZPool(lastSnapVlmDataRef.getStorPool())).append("/")
                .append(asSnapLvIdentifier(lastSnapVlmDataRef)).append(" ");
        }
        sb.append(getZPool(curSnapVlmDataRef.getStorPool())).append("/")
            .append(asSnapLvIdentifier(curSnapVlmDataRef));
        return sb.toString();
    }

    @Override
    protected void finishShipReceiving(ZfsData<Resource> vlmDataRef, ZfsData<Snapshot> snapVlmRef)
        throws StorageException, DatabaseException, AccessDeniedException
    {
        vlmDataRef.setInitialShipment(false);
        // String vlmDataId = asLvIdentifier(vlmDataRef);
        // deleteLvImpl(vlmDataRef, vlmDataId); // delete calls "vlmData.setExists(false);" - we have to undo this
        // ZfsCommands.rename(
        // extCmdFactory.create(),
        // vlmDataRef.getZPool(),
        // asSnapLvIdentifier(snapVlmRef),
        // vlmDataId
        // );
        // vlmDataRef.setExists(true);
    }

    @Override
    protected void updateStates(List<ZfsData<Resource>> vlmDataList, List<ZfsData<Snapshot>> snapVlms)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        Set<StorPool> storPools = new TreeSet<>();

        List<ZfsData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataList);
        combinedList.addAll(snapVlms);

        /*
         *  updating volume states
         */
        for (ZfsData<?> vlmData : combinedList)
        {
            storPools.add(vlmData.getStorPool());

            vlmData.setZPool(getZPool(vlmData.getStorPool()));
            vlmData.setIdentifier(asIdentifierRaw(vlmData));
            ZfsInfo info = infoListCache.get(vlmData.getFullQualifiedLvIdentifier());

            if (info != null)
            {
                updateInfo(vlmData, info);

                final long expectedSize = vlmData.getExpectedSize();
                final long actualSize = info.usableSize;
                if (actualSize != expectedSize && vlmData.getRscLayerObject().getAbsResource() instanceof Resource)
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

    @SuppressWarnings("unchecked")
    private void updateInfo(ZfsData<?> vlmData, ZfsInfo zfsInfo) throws DatabaseException, AccessDeniedException
    {
        vlmData.setExists(true);
        vlmData.setZPool(zfsInfo.poolName);
        vlmData.setIdentifier(zfsInfo.identifier);
        vlmData.setAllocatedSize(zfsInfo.allocatedSize);
        vlmData.setUsableSize(zfsInfo.usableSize);
        vlmData.setExtentSize(zfsInfo.volBlockSize);

        String devicePath;

        if (vlmData.getRscLayerObject().getAbsResource() instanceof Resource &&
            isCloning((ZfsData<Resource>) vlmData))
        {
            /*
             * while the volume is cloning, we do not want to set the device path so that other tools do not try to
             * access it
             */
            devicePath = null;
        }
        else
        {
            devicePath = zfsInfo.path;
        }

        vlmData.setDevicePath(devicePath);
    }

    protected PriorityProps getPrioProps(ZfsData<Resource> vlmDataRef) throws AccessDeniedException
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
    @Override
    protected void setDevicePath(ZfsData<Resource> vlmDataRef, String devicePathRef) throws DatabaseException
    {
        vlmDataRef.setDevicePath(devicePathRef);
    }

    @Override
    protected void setAllocatedSize(ZfsData<Resource> vlmDataRef, long sizeRef) throws DatabaseException
    {
        vlmDataRef.setAllocatedSize(sizeRef);
    }

    @Override
    protected void setUsableSize(ZfsData<Resource> vlmDataRef, long sizeRef) throws DatabaseException
    {
        vlmDataRef.setUsableSize(sizeRef);
    }

    @Override
    protected void setExpectedUsableSize(ZfsData<Resource> vlmData, long size)
        throws DatabaseException, StorageException
    {
        vlmData.setExpectedSize(size);
    }

    @Override
    protected String getStorageName(ZfsData<Resource> vlmDataRef) throws DatabaseException
    {
        return vlmDataRef.getZPool();
    }

    @Override
    protected void createLvWithCopyImpl(
        ZfsData<Resource> vlmData,
        Resource srcRsc)
        throws StorageException, AccessDeniedException
    {
        final String useZfsClone = vlmData.getRscLayerObject().getAbsResource()
            .getResourceDefinition().getProps(storDriverAccCtx).getProp(InternalApiConsts.KEY_USE_ZFS_CLONE);
        final ZfsData<Resource> srcVlmData = getVlmDataFromResource(
            srcRsc, vlmData.getRscLayerObject().getResourceNameSuffix(), vlmData.getVlmNr());

        final String dstRscName = vlmData.getRscLayerObject().getResourceName().displayValue;
        final String srcFullSnapshotName = getCloneSnapshotNameFull(srcVlmData, vlmData, "@");
        final String dstId = asLvIdentifier(vlmData);


        if (!infoListCache.containsKey(srcVlmData.getZPool() + "/" + srcFullSnapshotName))
        {
            ZfsCommands.createSnapshotFullName(
                extCmdFactory.create(),
                srcVlmData.getZPool(),
                srcFullSnapshotName
            );
            // mark snapshot as temporary clone
            ZfsCommands.setUserProperty(
                extCmdFactory.create(),
                srcVlmData.getZPool(),
                srcFullSnapshotName,
                "clone_for",
                dstRscName
            );

            if (useZfsClone != null)
            {
                ZfsCommands.restoreSnapshotFullName(
                    extCmdFactory.create(),
                    srcVlmData.getZPool(),
                    srcFullSnapshotName,
                    dstId
                );
            }
        }
        else
        {
            errorReporter.logInfo("Clone base snapshot %s already found, reusing.", srcFullSnapshotName);
        }

        cloneService.startClone(
            srcVlmData,
            vlmData,
            this);
    }

    @Override
    public String[] getCloneCommand(CloneService.CloneInfo cloneInfo)
    {
        String[] cmd;
        String useZfsClone = null;
        try
        {
            useZfsClone = cloneInfo.getDstVlmData().getRscLayerObject().getAbsResource()
                .getResourceDefinition().getProps(storDriverAccCtx).getProp(InternalApiConsts.KEY_USE_ZFS_CLONE);
        }
        catch (AccessDeniedException accessDeniedException)
        {
            errorReporter.reportError(accessDeniedException);
        }

        if (useZfsClone == null)
        {
            ZfsData<Resource> srcData = (ZfsData<Resource>) cloneInfo.getSrcVlmData();
            ZfsData<Resource> dstData = (ZfsData<Resource>) cloneInfo.getDstVlmData();
            final String dstId = asLvIdentifier(dstData);
            final String srcFullSnapshotName = getCloneSnapshotNameFull(srcData, dstData, "@");
            cmd = new String[]
                {
                    "timeout",
                    "0",
                    "bash",
                    "-c",
                    String.format(
                            "set -o pipefail; " +
                            "zfs send --embed --dedup --large-block %s/%s | " +
                            // if send/recv fails no new volume will be there, so destroy isn't needed
                            "zfs receive -F %s/%s && zfs destroy -r %s/%s@%%",
                        srcData.getZPool(),
                        srcFullSnapshotName,
                        dstData.getZPool(),
                        dstId,
                        dstData.getZPool(),
                        dstId)
                };
        }
        else
        {
            cmd = new String[]{"sleep", "1"};
        }
        return cmd;
    }

    @Override
    public void doCloneCleanup(CloneService.CloneInfo cloneInfo) throws StorageException
    {
        try
        {
            String useZfsClone = cloneInfo.getDstVlmData().getRscLayerObject().getAbsResource()
                .getResourceDefinition().getProps(storDriverAccCtx).getProp(InternalApiConsts.KEY_USE_ZFS_CLONE);

            // we cannot delete the snapshot if we have a dependent clone from it
            // so only delete if it was a send/recv clone
            // the snapshot will be tried to delete if the cloned resource gets removed
            if (useZfsClone == null)
            {
                ZfsData<Resource> srcData = (ZfsData<Resource>) cloneInfo.getSrcVlmData();
                final String srcFullSnapshotName = getCloneSnapshotNameFull(
                    srcData, (ZfsData<Resource>) cloneInfo.getDstVlmData(), "@");
                ZfsCommands.delete(
                    extCmdFactory.create(),
                    getZPool(srcData.getStorPool()),
                    srcFullSnapshotName,
                    ZfsVolumeType.SNAPSHOT
                );
            }
        }
        catch (AccessDeniedException accessDeniedException)
        {
            errorReporter.reportError(accessDeniedException);
        }
    }
}
