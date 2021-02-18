package com.linbit.linstor.layer.storage.zfs;

import com.linbit.ImplementationError;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.backupshipping.BackupShippingService;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.devmgr.pojos.LocalNodePropsChangePojo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.DeviceLayerUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.storage.utils.MkfsUtils;
import com.linbit.linstor.layer.storage.utils.PmemUtils;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsCommands;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsUtils;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsUtils.ZfsInfo;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
public class ZfsProvider extends AbsStorageProvider<ZfsInfo, ZfsData<Resource>, ZfsData<Snapshot>>
{
    protected static final int DEFAULT_ZFS_EXTENT_SIZE = 8; // 8K

    // FIXME: FORMAT should be private, only made public for LayeredSnapshotHelper
    public static final String FORMAT_RSC_TO_ZFS_ID = "%s%s_%05d";
    public static final String FORMAT_SNAP_TO_ZFS_ID = FORMAT_RSC_TO_ZFS_ID + "@%s";
    private static final String FORMAT_ZFS_DEV_PATH = "/dev/zvol/%s/%s";
    private static final int TOLERANCE_FACTOR = 3;

    private static final Pattern PATTERN_EXTENT_SIZE = Pattern.compile("(\\d+)\\s*(.*)");

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
        BackupShippingService backupShipMgrRef
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
        BackupShippingService backupShipMgrRef
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
        return ZfsUtils.getZfsList(extCmdFactory.create());
    }

    @Override
    protected String asSnapLvIdentifierRaw(String rscNameRef, String rscNameSuffixRef, String snapNameRef, int vlmNrRef)
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
    protected String asLvIdentifier(ResourceName resourceName, String rscNameSuffix, VolumeNumber volumeNumber)
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

    private String asLvIdentifier(String rscNameSuffix, SnapshotVolume snapVlm)
    {
        return asLvIdentifier(rscNameSuffix, snapVlm.getSnapshotVolumeDefinition()) + "@" +
            snapVlm.getSnapshotName().displayValue;
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
        String[] additionalOptionsArr;

        try
        {
            String options = getPrioProps(vlmDataRef).getProp(
                ApiConsts.KEY_STOR_POOL_ZFS_CREATE_OPTIONS,
                ApiConsts.NAMESPC_STORAGE_DRIVER,
                ""
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

    protected long roundUpToExtentSize(ZfsData<Resource> vlmDataRef, long sizeRef) throws StorageException
    {
        long volumeSize = sizeRef;

        long extentSize = getExtentSize(vlmDataRef);

        if (volumeSize % extentSize != 0)
        {
            long origSize = volumeSize;
            volumeSize = ((volumeSize / extentSize) + 1) * extentSize;
            errorReporter.logInfo(
                String.format(
                    "Aligning size from %d KiB to %d KiB to be a multiple of extent size %d KiB",
                    origSize,
                    volumeSize,
                    extentSize
                )
            );
        }
        return volumeSize;
    }

    private long getExtentSize(ZfsData<Resource> vlmDataRef) throws StorageException
    {
        long extentSize = DEFAULT_ZFS_EXTENT_SIZE;
        String[] zfscreateOptions = getZfscreateOptions(vlmDataRef);
        try
        {
            for (int i = 0; i < zfscreateOptions.length; i++)
            {
                String opt = zfscreateOptions[i];
                String extSizeStr;

                /*
                 * might be {..., "-b", "32k", ... } but also {..., "-b32k", ... }
                 */
                if (opt.equals("-b"))
                {
                    extSizeStr = zfscreateOptions[i + 1];
                }
                else
                if (opt.startsWith("-b"))
                {
                    extSizeStr = opt;
                }
                else
                if (opt.equals("-o"))
                {
                    extSizeStr = zfscreateOptions[i + 1].startsWith("volblocksize=") ?
                        zfscreateOptions[i + 1] :
                        null;
                }
                else
                if (opt.startsWith("-ovolblocksize="))
                {
                    extSizeStr = opt;
                }
                else
                {
                    extSizeStr = null;
                }


                if (extSizeStr != null)
                {
                    Matcher matcher = PATTERN_EXTENT_SIZE.matcher(extSizeStr);
                    if (matcher.find())
                    {
                        long val = Long.parseLong(matcher.group(1));
                        SizeUnit unit = SizeUnit.parse(matcher.group(2), true);

                        extentSize = SizeConv.convert(val, unit, SizeUnit.UNIT_KiB);
                    }
                    else
                    {
                        throw new StorageException("Could not find blocksize in string: " + extSizeStr);
                    }
                    break;
                }
            }
        }
        catch (ArrayIndexOutOfBoundsException aioobe) {
            throw new StorageException(
                "Expected additional argument while looking for extentSize in: " + Arrays.toString(zfscreateOptions)
            );
        }
        catch (NumberFormatException nfe)
        {
            throw new StorageException("Could not parse blocksize", nfe);
        }
        catch (IllegalArgumentException exc)
        {
            throw new StorageException("Could not parse blocksize unit ", exc);
        }

        return extentSize;
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
            lvId
        );
        vlmData.setExists(false);
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
                snapVlm.getVolume().getAbsResource().getSnapshotName().displayValue
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
    protected void deleteSnapshot(ZfsData<Snapshot> snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.delete(
            extCmdFactory.create(),
            getZPool(snapVlmRef.getStorPool()),
            asSnapLvIdentifier(snapVlmRef)
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
    protected String getStorageName(StorPool storPoolRef) throws AccessDeniedException
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
    public LocalNodePropsChangePojo checkConfig(StorPool storPool) throws StorageException, AccessDeniedException
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
        return null;
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
    public SpaceInfo getSpaceInfo(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String zPool = getZPool(storPool);
        if (zPool == null)
        {
            throw new StorageException("Unset zfs dataset for " + storPool);
        }
        int idx = zPool.indexOf(File.separator);
        if (idx == -1)
        {
            idx = zPool.length();
        }
        String rootPoolName = zPool.substring(0, idx);

        // do not use sub pool, we have to ask the actual zpool, not the sub dataset
        long capacity = ZfsUtils.getZPoolTotalSize(
            extCmdFactory.create(),
            Collections.singleton(rootPoolName)
        ).get(rootPoolName);
        long freeSpace = ZfsUtils.getZPoolFreeSize(
            extCmdFactory.create(),
            Collections.singleton(zPool)
        ).get(zPool);

        return new SpaceInfo(capacity, freeSpace);
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
        return "zfs receive -F " + getZPool(snapVlmDataRef.getStorPool()) + "/" + asSnapLvIdentifier(snapVlmDataRef);
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

    private void updateInfo(ZfsData<?> vlmData, ZfsInfo zfsInfo) throws DatabaseException
    {
        vlmData.setExists(true);
        vlmData.setZPool(zfsInfo.poolName);
        vlmData.setIdentifier(zfsInfo.identifier);
        vlmData.setAllocatedSize(zfsInfo.allocatedSize);
        vlmData.setUsableSize(zfsInfo.usableSize);
        vlmData.setDevicePath(zfsInfo.path);
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
        vlmData.setExepectedSize(
            roundUpToExtentSize(vlmData, size)
        );
    }

    @Override
    protected String getStorageName(ZfsData<Resource> vlmDataRef) throws DatabaseException
    {
        return vlmDataRef.getZPool();
    }

    @Override
    protected void createLvWithCopyImpl(
        ZfsData<Resource> vlmData,
        Resource srcRsc,
        String cloneSnapshotName)
        throws StorageException, AccessDeniedException
    {
        final ZfsData<Resource> srcVlmData = getVlmDataFromResource(
            srcRsc, vlmData.getRscLayerObject().getResourceNameSuffix(), vlmData.getVlmNr());

        final String dstRscName = vlmData.getRscLayerObject().getResourceName().displayValue;
        final String srcId = asLvIdentifier(srcVlmData);
        final String srcFullSnapshotName = srcId + "@" + cloneSnapshotName;


        if (!infoListCache.containsKey(srcVlmData.getZPool() + "/" + srcFullSnapshotName))
        {
            ZfsCommands.createSnapshot(
                extCmdFactory.create(),
                srcVlmData.getZPool(),
                srcId,
                cloneSnapshotName
            );
            // mark snapshot as temporary clone
            ZfsCommands.setUserProperty(
                extCmdFactory.create(),
                srcVlmData.getZPool(),
                srcFullSnapshotName,
                "clone_for",
                dstRscName
            );
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
    public String[] getCloneCommand(CloneService.CloneInfo cloneInfo) {
        ZfsData<Resource> srcData = (ZfsData<Resource>)cloneInfo.getSrcVlmData();
        ZfsData<Resource> dstData = (ZfsData<Resource>)cloneInfo.getDstVlmData();
        final String cloneSnapshotName = "clone_for_" + cloneInfo.getResourceName();
        final String srcId = asLvIdentifier(srcData);
        final String srcFullSnapshotName = srcId + "@" + cloneSnapshotName;
        final String dstId = asLvIdentifier(dstData);
        return new String[]
            {
                "setsid", "-w",
                "bash",
                "-c",
                String.format(
                    "trap 'kill -HUP 0' SIGTERM; " +
                        "set -o pipefail; " +
                        "(" +
                        "zfs send --embed --dedup --large-block %s/%s | " +
                        // if send/recv fails no new volume will be there, so destroy isn't needed
                        "zfs receive -F %s/%s && zfs destroy -r %s/%s@%% ;" +
                        ")& wait $!",
                    srcData.getZPool(),
                    srcFullSnapshotName,
                    dstData.getZPool(),
                    dstId,
                    dstData.getZPool(),
                    dstId)
            };
    }

    @Override
    public void doCloneCleanup(CloneService.CloneInfo cloneInfo) throws StorageException
    {
        ZfsData<Resource> srcData = (ZfsData<Resource>)cloneInfo.getSrcVlmData();
        final String cloneSnapshotName = "clone_for_" + cloneInfo.getResourceName();
        final String srcId = asLvIdentifier(srcData);
        final String srcFullSnapshotName = srcId + "@" + cloneSnapshotName;
        ZfsCommands.delete(extCmdFactory.create(), getZPool(srcData.getStorPool()), srcFullSnapshotName);
    }
}
