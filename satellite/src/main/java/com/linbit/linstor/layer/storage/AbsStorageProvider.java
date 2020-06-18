package com.linbit.linstor.layer.storage;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.fsevent.FileObserver;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.fsevent.FileSystemWatch.Event;
import com.linbit.fsevent.FileSystemWatch.FileEntry;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.storage.spdk.utils.SpdkCommands;
import com.linbit.linstor.layer.storage.utils.DmStatCommands;
import com.linbit.linstor.layer.storage.utils.StltProviderUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingManager;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.utils.AccessUtils;
import com.linbit.utils.ExceptionThrowingSupplier;
import com.linbit.utils.Pair;

import static com.linbit.linstor.layer.storage.spdk.utils.SpdkUtils.SPDK_PATH_PREFIX;

import javax.inject.Provider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public abstract class AbsStorageProvider<INFO, LAYER_DATA extends AbsStorageVlmData<Resource>, LAYER_SNAP_DATA extends AbsStorageVlmData<Snapshot>>
    implements DeviceProvider
{
    private static final long DFLT_WAIT_UNTIL_DEVICE_CREATED_TIMEOUT_IN_MS = 500;
    public static final long SIZE_OF_NOT_FOUND_STOR_POOL = -1;

    protected final ErrorReporter errorReporter;
    protected final ExtCmdFactory extCmdFactory;
    protected final AccessContext storDriverAccCtx;
    protected final Provider<NotificationListener> notificationListenerProvider;
    protected final Provider<TransactionMgr> transMgrProvider;
    protected final WipeHandler wipeHandler;
    protected final StltConfigAccessor stltConfigAccessor;
    protected Props localNodeProps;
    private final SnapshotShippingManager snapShipMgr;

    protected final HashMap<String, INFO> infoListCache;
    protected final List<Consumer<Map<String, Long>>> postRunVolumeNotifications = new ArrayList<>();
    protected final Set<String> changedStoragePoolStrings = new HashSet<>();
    private final String typeDescr;
    private final FileSystemWatch fsWatch;
    protected final DeviceProviderKind kind;

    private final Set<StorPool> changedStorPools = new HashSet<>();
    private boolean prepared;

    public AbsStorageProvider(
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        AccessContext storDriverAccCtxRef,
        StltConfigAccessor stltConfigAccessorRef,
        WipeHandler wipeHandlerRef,
        Provider<NotificationListener> notificationListenerProviderRef,
        Provider<TransactionMgr> transMgrProviderRef,
        String typeDescrRef,
        DeviceProviderKind kindRef,
        SnapshotShippingManager snapShipMgrRef
    )
    {
        errorReporter = errorReporterRef;
        extCmdFactory = extCmdFactoryRef;
        storDriverAccCtx = storDriverAccCtxRef;
        wipeHandler = wipeHandlerRef;
        notificationListenerProvider = notificationListenerProviderRef;
        stltConfigAccessor = stltConfigAccessorRef;
        transMgrProvider = transMgrProviderRef;
        typeDescr = typeDescrRef;
        kind = kindRef;
        snapShipMgr = snapShipMgrRef;

        infoListCache = new HashMap<>();
        try
        {
            fsWatch = new FileSystemWatch(errorReporter);
        }
        catch (IOException exc)
        {
            throw new LinStorRuntimeException("Unable to create FileSystemWatch", exc);
        }
    }

    @Override
    public void clearCache() throws StorageException
    {
        clearCache(true);
    }

    private void clearCache(boolean processPostRunVolumeNotifications) throws StorageException
    {
        infoListCache.clear();

        if (processPostRunVolumeNotifications && !changedStoragePoolStrings.isEmpty())
        {
            Map<String, Long> vgFreeSizes = getFreeSpacesImpl();
            postRunVolumeNotifications.forEach(consumer -> consumer.accept(vgFreeSizes));
        }
        changedStorPools.clear();
        changedStoragePoolStrings.clear();
        postRunVolumeNotifications.clear();

        prepared = false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(List<VlmProviderObject<Resource>> rawVlmDataList, List<VlmProviderObject<Snapshot>> rawSnapVlms)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        clearCache(false);

        Object intentionalTypeEreasure = rawVlmDataList;
        List<LAYER_DATA> vlmDataList = (List<LAYER_DATA>) intentionalTypeEreasure;

        intentionalTypeEreasure = rawSnapVlms;
        List<LAYER_SNAP_DATA> snapVlmDatList = (List<LAYER_SNAP_DATA>) intentionalTypeEreasure;

        updateVolumeAndSnapshotStates(vlmDataList, snapVlmDatList);

        prepared = true;
    }

    private void updateVolumeAndSnapshotStates(List<LAYER_DATA> vlmDataList, List<LAYER_SNAP_DATA> snapVlms)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        infoListCache.putAll(getInfoListImpl(vlmDataList, snapVlms));

        updateStates(vlmDataList, snapVlms);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(
        List<VlmProviderObject<Resource>> rawVlmDataList,
        List<VlmProviderObject<Snapshot>> snapshotVlms,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException, DatabaseException, StorageException
    {
        if (!prepared)
        {
            throw new ImplementationError("Process was called without previous prepare()");
        }
        Object intentionalTypeEreasure = rawVlmDataList;
        List<LAYER_DATA> vlmDataList = (List<LAYER_DATA>) intentionalTypeEreasure;

        Map<Boolean, List<VlmProviderObject<Snapshot>>> groupedSnapshotVolumesByDeletingFlag = snapshotVlms.stream()
            .collect(
                Collectors.partitioningBy(
                    snapVlm -> AccessUtils.execPrivileged(
                        // explicit cast to make eclipse compiler happy
                        (ExceptionThrowingSupplier<Boolean, AccessDeniedException>)
                        () -> snapVlm.getVolume().getAbsResource().getFlags()
                            .isSet(storDriverAccCtx, Snapshot.Flags.DELETE)
                    )
                )
            );
        Map<Pair<String, VolumeNumber>, LAYER_DATA> volumesLut = new HashMap<>();

        List<LAYER_DATA> vlmsToCreate = new ArrayList<>();
        List<LAYER_DATA> vlmsToDelete = new ArrayList<>();
        List<LAYER_DATA> vlmsToResize = new ArrayList<>();
        List<LAYER_DATA> vlmsToCheckForRollback = new ArrayList<>();

        for (LAYER_DATA vlmData : vlmDataList)
        {
            volumesLut.put(
                new Pair<>(
                    vlmData.getRscLayerObject().getSuffixedResourceName(),
                    vlmData.getVlmNr()
                ),
                vlmData
            );

            boolean vlmShouldExist = !((Volume) vlmData.getVolume()).getFlags().isSet(
                storDriverAccCtx,
                Volume.Flags.DELETE
            );
            vlmShouldExist &= !vlmData.getRscLayerObject().getAbsResource().getStateFlags().isSet(
                storDriverAccCtx,
                Resource.Flags.DISK_REMOVING
            );

            String lvId = vlmData.getIdentifier();
            if (vlmData.exists())
            {
                errorReporter.logTrace("Lv %s found", lvId);
                if (!vlmShouldExist)
                {
                    errorReporter.logTrace("Lv %s will be deleted", lvId);
                    vlmsToDelete.add(vlmData);
                }
                else
                {
                    Size sizeState = vlmData.getSizeState();
                    if (
                        ((Volume) vlmData.getVolume()).getFlags().isSet(storDriverAccCtx, Volume.Flags.RESIZE) &&
                            (sizeState.equals(VlmProviderObject.Size.TOO_LARGE) || sizeState.equals(VlmProviderObject.Size.TOO_SMALL))
                    )
                    {
                        errorReporter.logTrace("Lv %s will be resized", lvId);
                        vlmsToResize.add(vlmData);
                    }
                    vlmsToCheckForRollback.add(vlmData);
                }
            }
            else
            {
                if (vlmShouldExist)
                {
                    errorReporter.logTrace("Lv %s will be created", lvId);
                    vlmsToCreate.add(vlmData);
                }
                else
                {
                    errorReporter.logTrace("Lv %s should be deleted but does not exist; no-op", lvId);
                }
            }
            // whatever happens, we have to report the free space of these storage pools even if no layer
            // actually changed anything. The controller simply expects a report of free sizes
            addChangedStorPool(vlmData.getStorPool());
        }

        /*
         * first we need to handle snapshots in DELETING state
         *
         * this should prevent the following error-scenario:
         * deleting a zfs resource still having a snapshot will fail.
         * if the user then tries to delete the snapshot, and this snapshot-deletion is not executed
         * before the resource-deletion, the snapshot will never gets deleted because the resource-deletion
         * will fail before the snapshot-deletion-attempt.
         */

        // intentional type erasure
        Object typeErasedList = groupedSnapshotVolumesByDeletingFlag.get(true);
        deleteSnapshots(
            volumesLut,
            (List<LAYER_SNAP_DATA>) typeErasedList,
            apiCallRc
        );

        createVolumes(vlmsToCreate, apiCallRc);
        resizeVolumes(vlmsToResize, apiCallRc);
        deleteVolumes(vlmsToDelete, apiCallRc);

        // intentional type erasure
        typeErasedList = groupedSnapshotVolumesByDeletingFlag.get(false);
        takeSnapshots(
            volumesLut,
            (List<LAYER_SNAP_DATA>) typeErasedList,
            apiCallRc
        );

        handleRollbacks(vlmsToCheckForRollback, apiCallRc);
    }

    @Override
    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }

    protected Optional<String> getMigrationId(VolumeDefinition vlmDfn)
    {
        String overrideId;
        try
        {
            overrideId = vlmDfn.getProps(storDriverAccCtx).getProp(ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID);
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return Optional.ofNullable(overrideId);
    }

    protected Optional<String> getMigrationId(SnapshotVolumeDefinition snapVlmDfn)
    {
        String overrideId;
        try
        {
            overrideId = snapVlmDfn.getProps(storDriverAccCtx).getProp(ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID);
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return Optional.ofNullable(overrideId);
    }

    @Override
    public Collection<StorPool> getChangedStorPools()
    {
        Set<StorPool> copy = new HashSet<>(changedStorPools);
        return copy;
    }

    private void createVolumes(List<LAYER_DATA> vlmsToCreate, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        for (LAYER_DATA vlmData : vlmsToCreate)
        {
            String sourceLvId = computeRestoreFromResourceName(vlmData);
            // sourceLvId ends with "_00000"

            String sourceSnapshotName = computeRestoreFromSnapshotName(vlmData.getVolume());

            boolean snapRestore = sourceLvId != null && sourceSnapshotName != null;
            if (snapRestore)
            {
                errorReporter.logTrace("Restoring from lv: %s, snapshot: %s", sourceLvId, sourceSnapshotName);
                restoreSnapshot(sourceLvId, sourceSnapshotName, vlmData);
            }
            else
            {
                createLvImpl(vlmData);
            }
            vlmData.setExists(true);

            String storageName = getStorageName(vlmData);
            String lvId = asLvIdentifier(vlmData);

            // some providers cannot construct a device path in the next call and therefore return null here
            String devicePath = getDevicePath(storageName, lvId);

            // those providers will most likely also skip setting the (null) devicePath.
            setDevicePath(vlmData, devicePath);

            // however, those providers have to have a different method in getting the device path which means
            // the correct device path was already set since the "createLvImpl" or "restoreSnapshot" call.
            devicePath = vlmData.getDevicePath();

            StorPool storPool = vlmData.getStorPool();
            long waitTimeoutAfterCreate = getWaitTimeoutAfterCreate(storPool);
            waitUntilDeviceCreated(devicePath, waitTimeoutAfterCreate);

            long allocatedSize = getAllocatedSize(vlmData);
            long minSize = kind.usesThinProvisioning() ? 0 : vlmData.getExepectedSize();
            if (allocatedSize < minSize)
            {
                throw new StorageException("Size of create volume is too low. Expected " +
                    minSize + ". Actual: " + allocatedSize + ". Volume: " + vlmData);
            }

            setAllocatedSize(vlmData, allocatedSize);
            setUsableSize(vlmData, allocatedSize);

            if (stltConfigAccessor.useDmStats() && updateDmStats())
            {
                DmStatCommands.create(extCmdFactory.create(), devicePath);
            }

            if (!snapRestore && !devicePath.startsWith(SPDK_PATH_PREFIX))
            {
                wipeHandler.quickWipe(devicePath);
            }

            addCreatedMsg(vlmData, apiCallRc);
        }
    }

    protected long getWaitTimeoutAfterCreate(StorPool storPoolRef)
        throws AccessDeniedException
    {
        long timeout = DFLT_WAIT_UNTIL_DEVICE_CREATED_TIMEOUT_IN_MS;
        try
        {
            String timeoutStr = new PriorityProps(
                storPoolRef.getProps(storDriverAccCtx),
                stltConfigAccessor.getReadonlyProps()
            ).getProp(
                ApiConsts.KEY_STOR_POOL_WAIT_TIMEOUT_AFTER_CREATE,
                ApiConsts.NAMESPC_STORAGE_DRIVER
            );
            if (timeoutStr != null)
            {
                timeout = Long.parseLong(timeoutStr);
            }
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return timeout;
    }

    protected void addChangedStorPool(StorPool storPoolRef) throws AccessDeniedException, StorageException
    {
        changedStorPools.add(storPoolRef);
        changedStoragePoolStrings.add(getStorageName(storPoolRef));
    }

    private void resizeVolumes(List<LAYER_DATA> vlmsToResize, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        for (LAYER_DATA vlmData : vlmsToResize)
        {
            resizeLvImpl(vlmData);

            long allocatedSize = getAllocatedSize(vlmData);
            setAllocatedSize(vlmData, allocatedSize);
            setUsableSize(vlmData, allocatedSize);

            addResizedMsg(vlmData, apiCallRc);
        }
    }

    private void deleteVolumes(List<LAYER_DATA> vlmsToDelete, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, StorageException, DatabaseException
    {
        for (LAYER_DATA vlmData : vlmsToDelete)
        {
            String lvId = asLvIdentifier(vlmData);

            if (stltConfigAccessor.useDmStats() && updateDmStats())
            {
                DmStatCommands.delete(extCmdFactory.create(), vlmData.getDevicePath());
            }

            deleteLvImpl(vlmData, lvId);

            if (!vlmData.getVolume().getAbsResource().getStateFlags().isSet(
                storDriverAccCtx,
                Resource.Flags.DISK_REMOVING)
            )
            {
                addDeletedMsg(vlmData, apiCallRc);
            }
        }
    }

    private void deleteSnapshots(
        Map<Pair<String, VolumeNumber>, LAYER_DATA> vlmDataLut,
        List<LAYER_SNAP_DATA> snapVlmsDataList,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        for (LAYER_SNAP_DATA snapVlm : snapVlmsDataList)
        {
            Snapshot snap = snapVlm.getRscLayerObject().getAbsResource();
            if (snap.getFlags().isSet(storDriverAccCtx, Snapshot.Flags.SHIPPING_TARGET))
            {
                LAYER_DATA vlmData = vlmDataLut.get(
                    new Pair<>(
                        snapVlm.getRscLayerObject().getSuffixedResourceName(),
                        snapVlm.getVlmNr()
                    )
                );
                if (vlmData == null)
                {
                    throw new StorageException(
                        String.format(
                            "Could not merge snapshot '%s' into its volume as the volume was not found.",
                            snapVlm.toString()
                        )
                    );
                }
                errorReporter.logTrace("Post shipping cleanup for snapshot %s", snapVlm.toString());
                finishShipReceiving(vlmData, snapVlm);

            }
            else // deleting the source should be the same as deleting an ordinary snapshot
            {
                errorReporter.logTrace("Deleting snapshot %s", snapVlm.toString());
                if (snapshotExists(snapVlm))
                {
                    deleteSnapshot(snapVlm);
                }
                else
                {
                    errorReporter.logTrace("Snapshot '%s' already deleted", snapVlm.toString());
                }

                addSnapDeletedMsg(snapVlm, apiCallRc);
            }

        }
    }

    private void takeSnapshots(
        Map<Pair<String, VolumeNumber>, LAYER_DATA> vlmDataLut,
        List<LAYER_SNAP_DATA> listRef,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        for (LAYER_SNAP_DATA snapVlm : listRef)
        {
            if (snapVlm.getVolume().getAbsResource().getTakeSnapshot(storDriverAccCtx))
            {
                LAYER_DATA vlmData = vlmDataLut.get(
                    new Pair<>(
                        snapVlm.getRscLayerObject().getSuffixedResourceName(),
                        snapVlm.getVlmNr()
                    )
                );
                if (vlmData == null)
                {
                    throw new StorageException(
                        String.format(
                            "Could not create snapshot '%s' as there is no corresponding volume.",
                            snapVlm.toString()
                        )
                    );
                }
                if (!snapshotExists(snapVlm))
                {
                    errorReporter.logTrace("Taking snapshot %s", snapVlm.toString());
                    createSnapshot(vlmData, snapVlm);

                    addSnapCreatedMsg(snapVlm, apiCallRc);

                    Snapshot snap = snapVlm.getVolume().getAbsResource();
                    if (snap.getFlags().isSet(storDriverAccCtx, Snapshot.Flags.SHIPPING_TARGET))
                    {
                        StorPool storPool = vlmData.getStorPool();
                        long waitTimeoutAfterCreate = getWaitTimeoutAfterCreate(storPool);

                        String snapId = asSnapLvIdentifier(snapVlm);
                        String storageName = getStorageName(vlmData);
                        String devicePath = getDevicePath(storageName, snapId);

                        waitUntilDeviceCreated(devicePath, waitTimeoutAfterCreate);

                        startReceiving(vlmData, snapVlm);
                    }
                }
                else
                {
                    try
                    {
                        Snapshot snap = snapVlm.getVolume().getAbsResource();
                        if (snap.getFlags().isSet(storDriverAccCtx, Snapshot.Flags.SHIPPING_SOURCE_START))
                        {
                            startSending(snapVlm);
                        }
                    }
                    catch (InvalidNameException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                }
            }
        }
    }

    private LAYER_SNAP_DATA getPreviousSnapvlmData(
        LAYER_SNAP_DATA snapVlm,
        ResourceConnection rscCon
    )
        throws AccessDeniedException, InvalidNameException
    {
        LAYER_SNAP_DATA prevSnapVlmData = null;

        String prevSnapName = rscCon.getProps(storDriverAccCtx)
            .getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_NAME_PREV);
        if (prevSnapName != null)
        {
            Snapshot snap = snapVlm.getVolume().getAbsResource();
            SnapshotDefinition prevSnapDfn = snap.getResourceDefinition()
                .getSnapshotDfn(storDriverAccCtx, new SnapshotName(prevSnapName));
            if (prevSnapDfn != null)
            {
                AbsRscLayerObject<Snapshot> prevSnapLayerData = prevSnapDfn
                    .getSnapshot(storDriverAccCtx, snap.getNodeName())
                    .getLayerData(storDriverAccCtx);

                Set<AbsRscLayerObject<Snapshot>> prevSnapStorageDataSet = LayerRscUtils
                    .getRscDataByProvider(prevSnapLayerData, DeviceLayerKind.STORAGE);

                AbsRscLayerObject<Snapshot> currentSnapLayerData = snapVlm.getRscLayerObject();
                String curSnapNameSuffix = currentSnapLayerData.getResourceNameSuffix();

                for (AbsRscLayerObject<Snapshot> prevSnapStorageData : prevSnapStorageDataSet)
                {
                    String prevSnapNameSuffix = prevSnapStorageData.getResourceNameSuffix();
                    if (prevSnapNameSuffix.equals(curSnapNameSuffix))
                    {
                        prevSnapVlmData = prevSnapLayerData.getVlmProviderObject(snapVlm.getVlmNr());
                        break;
                    }
                }
            }
        }

        return prevSnapVlmData;
    }

    protected void startReceiving(
        LAYER_DATA vlmDataRef,
        LAYER_SNAP_DATA snapVlmData
    )
        throws AccessDeniedException, StorageException, DatabaseException
    {
        Props snapDfnProps = snapVlmData.getRscLayerObject().getAbsResource().getSnapshotDefinition().getProps(
            storDriverAccCtx
        );
        String socatPort = snapDfnProps.getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_PORT);

        snapShipMgr.startReceiving(
            asSnapLvIdentifier(snapVlmData),
            getSnapshotShippingReceivingCommandImpl(snapVlmData),
            socatPort,
            snapVlmData
        );
    }

    protected void startSending(
        LAYER_SNAP_DATA curSnapVlmData
    )
        throws AccessDeniedException, StorageException, InvalidNameException
    {
        Snapshot snapSource = curSnapVlmData.getRscLayerObject().getAbsResource();

        SnapshotDefinition snapDfn = snapSource.getSnapshotDefinition();
        Props snapDfnProps = snapDfn.getProps(storDriverAccCtx);

        String socatPort = snapDfnProps.getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_PORT);
        String snapTargetName = snapDfnProps.getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_TARGET_NODE);

        NetInterface targetNetIf = getTargetNetIf(snapDfn, snapTargetName);

        Resource sourceRsc = snapSource.getResourceDefinition().getResource(storDriverAccCtx, snapSource.getNodeName());
        Resource targetRsc = targetNetIf.getNode().getResource(storDriverAccCtx, snapSource.getResourceName());

        ResourceConnection rscCon = sourceRsc.getAbsResourceConnection(storDriverAccCtx, targetRsc);

        LAYER_SNAP_DATA prevSnapVlmData = getPreviousSnapvlmData(curSnapVlmData, rscCon);

        snapShipMgr.startSending(
            asSnapLvIdentifier(curSnapVlmData),
            getSnapshotShippingSendingCommandImpl(prevSnapVlmData, curSnapVlmData),
            targetNetIf,
            socatPort,
            curSnapVlmData
        );
    }

    private NetInterface getTargetNetIf(SnapshotDefinition snapDfn, String snapTargetName)
        throws AccessDeniedException, ImplementationError
    {
        NetInterface targetNic;
        try
        {
            Node targetNode = snapDfn.getResourceDefinition()
                .getResource(storDriverAccCtx, new NodeName(snapTargetName)).getNode();

            PriorityProps targetNodePropProps = new PriorityProps(
                targetNode.getProps(storDriverAccCtx),
                stltConfigAccessor.getReadonlyProps()
            );
            String targetPrefNicStr = targetNodePropProps.getProp(
                InternalApiConsts.KEY_SNAPSHOT_SHIPPING_PREF_TARGET_NIC,
                "",
                ApiConsts.DEFAULT_NETIF
            );
            targetNic = targetNode.getNetInterface(storDriverAccCtx, new NetInterfaceName(targetPrefNicStr));
            if (targetNic == null)
            {
                if (!targetPrefNicStr.equalsIgnoreCase(ApiConsts.DEFAULT_NETIF))
                {
                    targetNic = targetNode.getNetInterface(
                        storDriverAccCtx,
                        new NetInterfaceName(ApiConsts.DEFAULT_NETIF)
                    );
                }
                if (targetNic == null)
                {
                    targetNic = targetNode.streamNetInterfaces(storDriverAccCtx).findAny().orElseThrow(
                        () -> new ImplementationError("No NetIfs available")
                    );
                }
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(invalidNameExc);
        }
        return targetNic;
    }

    private void handleRollbacks(List<LAYER_DATA> vlmsToCheckForRollback, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, StorageException, DatabaseException
    {
        for (LAYER_DATA vlmData : vlmsToCheckForRollback)
        {
            String rollbackTargetSnapshotName = vlmData.getVolume().getAbsResource()
                .getProps(storDriverAccCtx).map()
                .get(ApiConsts.KEY_RSC_ROLLBACK_TARGET);
            if (rollbackTargetSnapshotName != null)
            {
                rollbackImpl(vlmData, rollbackTargetSnapshotName);
            }
        }
    }

    /**
     * Default implementation performs a 'blockdev --getsize64 $devicePath'.
     * This method can be overridden by thin-providers to do different calculations
     *
     * @param vlmData
     * @return
     * @throws StorageException
     */
    protected long getAllocatedSize(LAYER_DATA vlmData) throws StorageException
    {
        return StltProviderUtils.getAllocatedSize(vlmData, extCmdFactory.create());
    }

    protected void addPostRunNotification(
        StorPool storPool,
        Consumer<Map<String, Long>> consumer
    )
        throws AccessDeniedException, StorageException
    {
        addChangedStorPool(storPool);
        postRunVolumeNotifications.add(consumer);
    }

    private void addCreatedMsg(LAYER_DATA vlmData, ApiCallRcImpl apiCallRc)
    {
        String rscName = vlmData.getRscLayerObject().getSuffixedResourceName();
        int vlmNr = vlmData.getVlmNr().value;
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.MASK_VLM | ApiConsts.CREATED,
                String.format(
                    "Volume number %d of resource '%s' [%s] created",
                    vlmNr,
                    rscName,
                    typeDescr
                )
            )
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName)
            .putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr))
            .build()
        );
    }

    private void addResizedMsg(LAYER_DATA vlmData, ApiCallRcImpl apiCallRc)
    {
        String rscName = vlmData.getRscLayerObject().getSuffixedResourceName();
        int vlmNr = vlmData.getVlmNr().value;
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.MASK_VLM | ApiConsts.MODIFIED,
                String.format(
                    "Volume number %d of resource '%s' [%s] resized",
                    vlmNr,
                    rscName,
                    typeDescr
                )
            )
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName)
            .putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr))
            .build()
        );
    }

    private void addDeletedMsg(LAYER_DATA vlmData, ApiCallRcImpl apiCallRc)
    {
        String rscName = vlmData.getRscLayerObject().getSuffixedResourceName();
        int vlmNr = vlmData.getVlmNr().value;
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.MASK_VLM | ApiConsts.DELETED,
                String.format(
                    "Volume number %d of resource '%s' [%s] deleted",
                    vlmNr,
                    rscName,
                    typeDescr
                )
            )
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName)
            .putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr))
            .build()
        );
    }

    private void addSnapCreatedMsg(LAYER_SNAP_DATA snapVlmData, ApiCallRcImpl apiCallRc)
    {
        Snapshot snap = snapVlmData.getVolume().getAbsResource();
        String snapName = snap.getSnapshotName().displayValue;
        String rscName = snap.getResourceName().displayValue;
        int vlmNr = snapVlmData.getVlmNr().value;

        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.MASK_SNAPSHOT | ApiConsts.CREATED,
                String.format(
                    "Snapshot [%s] with name '%s' of resource '%s', volume number %d created.",
                    typeDescr,
                    snapName,
                    rscName,
                    vlmNr
                )
            )
            .putObjRef(ApiConsts.KEY_SNAPSHOT, snapName)
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName)
            .putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr))
            .build()
        );
    }

    private void addSnapDeletedMsg(LAYER_SNAP_DATA snapVlmData, ApiCallRcImpl apiCallRc)
    {
        SnapshotVolume snapVlm = (SnapshotVolume) snapVlmData.getVolume();
        String snapName = snapVlm.getSnapshotName().displayValue;
        String rscName = snapVlm.getResourceName().displayValue;
        int vlmNr = snapVlm.getVolumeNumber().value;
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.MASK_SNAPSHOT | ApiConsts.DELETED,
                String.format(
                    "Snapshot [%s] with name '%s' of resource '%s', volume number %d deleted.",
                    typeDescr,
                    snapName,
                    rscName,
                    vlmNr
                )
            )
            .putObjRef(ApiConsts.KEY_SNAPSHOT, snapName)
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName)
            .putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr))
            .build()
        );
    }

    private String computeRestoreFromResourceName(LAYER_DATA vlmData)
        throws AccessDeniedException
    {
        String restoreVlmName;
        try
        {
            Props props = vlmData.getVolume().getProps(storDriverAccCtx);
            String restoreFromResourceName = props.getProp(ApiConsts.KEY_VLM_RESTORE_FROM_RESOURCE);

            if (restoreFromResourceName != null)
            {
                restoreVlmName =
                    getMigrationId(vlmData.getVolume().getVolumeDefinition())
                    .orElse(asLvIdentifier(
                        new ResourceName(restoreFromResourceName),
                        vlmData.getRscLayerObject().getResourceNameSuffix(),
                        vlmData.getVlmNr())
                    );
            }
            else
            {
                restoreVlmName = null;
            }
        }
        catch (InvalidKeyException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }

        return restoreVlmName;
    }

    private String computeRestoreFromSnapshotName(AbsVolume<Resource> vlm)
        throws AccessDeniedException
    {
        String restoreSnapshotName;
        try
        {
            Props props = vlm.getProps(storDriverAccCtx);
            String restoreFromSnapshotProp = props.getProp(ApiConsts.KEY_VLM_RESTORE_FROM_SNAPSHOT);

            restoreSnapshotName = restoreFromSnapshotProp != null ?
                // Parse into 'Name' objects in order to validate the property contents
                new SnapshotName(restoreFromSnapshotProp).displayValue : null;
        }
        catch (InvalidNameException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return restoreSnapshotName;
    }

    private void waitUntilDeviceCreated(String devicePath, long waitTimeoutAfterCreateMillis)
        throws StorageException
    {
        if (!devicePath.startsWith(SPDK_PATH_PREFIX))
        {
            waitUntilNonSpdkCreated(devicePath, waitTimeoutAfterCreateMillis);
        }
        else
        {
            // wait not required, just confirming LV existence
            SpdkCommands.lvsByName(extCmdFactory.create(), devicePath.split(SPDK_PATH_PREFIX)[1]);
        }
    }

    private void waitUntilNonSpdkCreated(String devicePath, long waitTimeoutAfterCreateMillis)
        throws StorageException
    {
        final Object syncObj = new Object();
        FileObserver fileObserver = new FileObserver()
        {
            @Override
            public void fileEvent(FileEntry watchEntry)
            {
                synchronized (syncObj)
                {
                    syncObj.notify();
                }
            }
        };
        try
        {
            synchronized (syncObj)
            {
                long start = System.currentTimeMillis();
                fsWatch.addFileEntry(
                    new FileEntry(
                        Paths.get(devicePath),
                        Event.CREATE,
                        fileObserver
                    )
                );
                try
                {
                    errorReporter.logTrace(
                        "Waiting until device [%s] appears (up to %dms)",
                        devicePath,
                        waitTimeoutAfterCreateMillis
                    );

                    syncObj.wait(waitTimeoutAfterCreateMillis);
                }
                catch (InterruptedException interruptedExc)
                {
                    throw new StorageException(
                        "Interrupted exception while waiting for device '" + devicePath + "' to show up",
                        interruptedExc
                    );
                }
                if (!Files.exists(Paths.get(devicePath)))
                {
                    throw new StorageException(
                        "Device '" + devicePath + "' did not show up in " +
                            waitTimeoutAfterCreateMillis + "ms"
                    );
                }
                errorReporter.logTrace(
                    "Device [%s] appeared after %sms",
                    devicePath,
                    System.currentTimeMillis() - start
                );
            }
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "Unable to register file watch event for device '" + devicePath + "' being created",
                exc
            );
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void updateGrossSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException
    {
        // usable size was just updated (set) by the layer above us. copy that, so we can
        // update it again with the actual usable size when we are finished
        setExpectedUsableSize((LAYER_DATA) vlmData, vlmData.getUsableSize());
    }

    @Override
    public void updateAllocatedSize(VlmProviderObject<Resource> vlmDataRef)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        LAYER_DATA vlmData = (LAYER_DATA) vlmDataRef;
        setAllocatedSize(vlmData, getAllocatedSize(vlmData));
    }

    @Override
    public abstract void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException;

    @Override
    public abstract SpaceInfo getSpaceInfo(StorPool storPool) throws StorageException, AccessDeniedException;

    protected abstract String getStorageName(StorPool storPoolRef) throws AccessDeniedException, StorageException;

    @SuppressWarnings("unused")
    protected void createSnapshot(LAYER_DATA vlmData, LAYER_SNAP_DATA snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @SuppressWarnings("unused")
    protected void restoreSnapshot(String sourceLvId, String sourceSnapName, LAYER_DATA vlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @SuppressWarnings("unused")
    protected void deleteSnapshot(LAYER_SNAP_DATA snapVlm)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @SuppressWarnings("unused")
    protected boolean snapshotExists(LAYER_SNAP_DATA snapVlm)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @SuppressWarnings("unused")
    protected void rollbackImpl(LAYER_DATA vlmData, String rollbackTargetSnapshotName)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    protected String getSnapshotShippingReceivingCommandImpl(LAYER_SNAP_DATA snapVlmDataRef)
        throws StorageException
    {
        throw new StorageException("Snapshot shipping is not supported by " + getClass().getSimpleName());
    }

    protected String getSnapshotShippingSendingCommandImpl(
        LAYER_SNAP_DATA lastSnapVlmDataRef,
        LAYER_SNAP_DATA curSnapVlmDataRef
    )
        throws StorageException
    {
        throw new StorageException("Snapshot shipping is not supported by " + getClass().getSimpleName());
    }

    protected void finishShipReceiving(LAYER_DATA vlmDataRef, LAYER_SNAP_DATA snapVlmRef)
        throws StorageException, DatabaseException
    {
        throw new StorageException("Snapshot shipping is not supported by " + getClass().getSimpleName());
    }

    protected abstract boolean updateDmStats();

    protected abstract Map<String, Long> getFreeSpacesImpl() throws StorageException;

    protected abstract Map<String, INFO> getInfoListImpl(
        List<LAYER_DATA> vlmDataList,
        List<LAYER_SNAP_DATA> snapVlmsRef
    )
        throws StorageException, AccessDeniedException, DatabaseException;

    protected abstract void updateStates(List<LAYER_DATA> vlmDataList, List<LAYER_SNAP_DATA> snapVlms)
        throws StorageException, AccessDeniedException, DatabaseException;

    protected abstract void createLvImpl(LAYER_DATA vlmData)
        throws StorageException, AccessDeniedException, DatabaseException;

    protected abstract void resizeLvImpl(LAYER_DATA vlmData)
        throws StorageException, AccessDeniedException, DatabaseException;

    protected abstract void deleteLvImpl(LAYER_DATA vlmData, String lvId)
        throws StorageException, AccessDeniedException, DatabaseException;


    protected String asLvIdentifier(LAYER_DATA vlmData)
    {
        return asLvIdentifier(
            vlmData.getRscLayerObject().getResourceNameSuffix(),
            vlmData.getVolume().getVolumeDefinition()
        );
    }

    protected String asSnapLvIdentifier(LAYER_SNAP_DATA snapVlmData)
    {
        return asSnapLvIdentifier(
            snapVlmData.getRscLayerObject().getResourceNameSuffix(),
            ((SnapshotVolume) snapVlmData.getVolume()).getSnapshotVolumeDefinition()
        );
    }

    protected String asLvIdentifier(String rscNameSuffix, VolumeDefinition vlmDfn)
    {
        return getMigrationId(vlmDfn).orElse(
            asLvIdentifier(
                vlmDfn.getResourceDefinition().getName(),
                rscNameSuffix,
                vlmDfn.getVolumeNumber()
            )
        );
    }

    protected String asSnapLvIdentifier(String rscNameSuffix, SnapshotVolumeDefinition snapVlmDfn)
    {
        return asSnapLvIdentifier(
            snapVlmDfn.getResourceName(),
            rscNameSuffix,
            snapVlmDfn.getSnapshotName(),
            snapVlmDfn.getVolumeNumber()
        );
    }

    protected String asLvIdentifier(String rscNameSuffix, SnapshotVolumeDefinition snapVlmDfn)
    {
        return getMigrationId(snapVlmDfn).orElse(
            asLvIdentifier(
                snapVlmDfn.getResourceName(),
                rscNameSuffix,
                snapVlmDfn.getVolumeNumber()
            )
        );
    }

    protected abstract String asLvIdentifier(
        ResourceName resourceName,
        String rscNameSuffix,
        VolumeNumber volumeNumber
    );

    protected String asSnapLvIdentifier(
        ResourceName rscName,
        String rscNameSuffix,
        SnapshotName snapName,
        VolumeNumber vlmNr
    )
    {
        return asSnapLvIdentifierRaw(
            rscName.displayValue,
            rscNameSuffix,
            snapName.displayValue,
            vlmNr.value
        );
    }

    protected abstract String asSnapLvIdentifierRaw(
        String rscName,
        String rscNameSuffix,
        String snapName,
        int vlmNr
    );

    protected abstract String getDevicePath(String storageName, String lvId);

    protected abstract String getStorageName(LAYER_DATA vlmData) throws DatabaseException;

    protected abstract void setDevicePath(LAYER_DATA vlmData, String devicePath) throws DatabaseException;

    protected abstract void setAllocatedSize(LAYER_DATA vlmData, long size) throws DatabaseException;

    protected abstract void setUsableSize(LAYER_DATA vlmData, long size) throws DatabaseException;

    protected abstract void setExpectedUsableSize(LAYER_DATA vlmData, long size) throws DatabaseException;
}
