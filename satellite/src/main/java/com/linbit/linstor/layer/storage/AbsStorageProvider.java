package com.linbit.linstor.layer.storage;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.Platform;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.backupshipping.AbsBackupShippingService;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition.Flags;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.storage.utils.DeviceUtils;
import com.linbit.linstor.layer.storage.utils.DmStatCommands;
import com.linbit.linstor.layer.storage.utils.SharedStorageUtils;
import com.linbit.linstor.layer.storage.utils.StltProviderUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public abstract class AbsStorageProvider<INFO, LAYER_DATA extends AbsStorageVlmData<Resource>, LAYER_SNAP_DATA extends AbsStorageVlmData<Snapshot>>
    implements DeviceProvider
{
    private static final long DFLT_WAIT_UNTIL_DEVICE_CREATED_TIMEOUT_IN_MS = 5000;
    public static final long SIZE_OF_NOT_FOUND_STOR_POOL = ApiConsts.VAL_STOR_POOL_SPACE_NOT_FOUND;

    private static final String CLONE_PREFIX = "CF_";

    protected final ErrorReporter errorReporter;
    protected final ExtCmdFactoryStlt extCmdFactory;
    protected final AccessContext storDriverAccCtx;
    protected final Provider<NotificationListener> notificationListenerProvider;
    protected final Provider<TransactionMgr> transMgrProvider;
    protected final WipeHandler wipeHandler;
    protected final StltConfigAccessor stltConfigAccessor;
    protected Props localNodeProps;
    private final SnapshotShippingService snapShipMgr;
    protected final CloneService cloneService;
    protected final StltExtToolsChecker extToolsChecker;
    private final BackupShippingMgr backupShipMapper;
    protected final HashMap<String, INFO> infoListCache;
    protected boolean subclassMaintainsInfoListCache;
    protected final List<Consumer<Map<String, Long>>> postRunVolumeNotifications = new ArrayList<>();
    protected final Set<String> changedStoragePoolStrings = new HashSet<>();
    private final String typeDescr;
    private final FileSystemWatch fsWatch;
    protected final DeviceProviderKind kind;

    private final Map<StorPool, Long> extentSizeFromSpCache = new HashMap<>();

    private final Set<StorPool> changedStorPools = new HashSet<>();
    private boolean prepared;
    protected boolean isDevPathExpectedToBeNull = false;

    public AbsStorageProvider(
        ErrorReporter errorReporterRef,
        ExtCmdFactoryStlt extCmdFactoryRef,
        AccessContext storDriverAccCtxRef,
        StltConfigAccessor stltConfigAccessorRef,
        WipeHandler wipeHandlerRef,
        Provider<NotificationListener> notificationListenerProviderRef,
        Provider<TransactionMgr> transMgrProviderRef,
        String typeDescrRef,
        DeviceProviderKind kindRef,
        SnapshotShippingService snapShipMgrRef,
        StltExtToolsChecker extToolsCheckerRef,
        CloneService cloneServiceRef,
        BackupShippingMgr backupShipMgrRef
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
        extToolsChecker = extToolsCheckerRef;
        cloneService = cloneServiceRef;
        backupShipMapper = backupShipMgrRef;

        subclassMaintainsInfoListCache = false;
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
    public void initialize()
    {
        if (kind.isSharedVolumeSupported())
        {
            extCmdFactory.setUsedWithSharedLock();
        }
    }

    @Override
    public void clearCache() throws StorageException
    {
        clearCache(true);
    }

    private void clearCache(boolean processPostRunVolumeNotifications) throws StorageException
    {
        if (!subclassMaintainsInfoListCache)
        {
            infoListCache.clear();
        }

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
        Map<String, INFO> currentInfo = getInfoListImpl(vlmDataList, snapVlms);
        if (!subclassMaintainsInfoListCache)
        {
            infoListCache.putAll(currentInfo);
        }   /* else this already has been done */

        updateStates(vlmDataList, snapVlms);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processVolumes(List<VlmProviderObject<Resource>> rawVlmDataList, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        if (!prepared)
        {
            throw new ImplementationError("Process was called without previous prepare()");
        }
        Object intentionalTypeErasure = rawVlmDataList;
        List<LAYER_DATA> vlmDataList = (List<LAYER_DATA>) intentionalTypeErasure;

        List<LAYER_DATA> vlmsToCreate = new ArrayList<>();
        List<LAYER_DATA> vlmsToDelete = new ArrayList<>();
        List<LAYER_DATA> vlmsToDeactivate = new ArrayList<>();
        List<LAYER_DATA> vlmsToResize = new ArrayList<>();
        List<LAYER_DATA> vlmsToCheckForRollback = new ArrayList<>();

        for (LAYER_DATA vlmData : vlmDataList)
        {
            // Whatever happens, we have to report the free space of these storage pools even if no layer
            // actually changed anything. The controller simply expects a report of free sizes
            addChangedStorPool(vlmData.getStorPool());

            boolean vlmShouldExist = !((Volume) vlmData.getVolume()).getFlags().isSet(
                storDriverAccCtx,
                Volume.Flags.DELETE
            );
            StateFlags<Resource.Flags> rscFlags = vlmData.getRscLayerObject().getAbsResource().getStateFlags();
            vlmShouldExist &= !rscFlags.isSet(
                storDriverAccCtx,
                Resource.Flags.DISK_REMOVING
            );
            boolean vlmShouldBeActive = rscFlags.isUnset(storDriverAccCtx, Resource.Flags.INACTIVE);

            String lvId = vlmData.getIdentifier();
            if (vlmData.exists())
            {
                if (!cloneService.isRunning(
                        vlmData.getRscLayerObject().getResourceName(),
                        vlmData.getVlmNr(),
                        vlmData.getRscLayerObject().getResourceNameSuffix()))
                {
                    errorReporter.logTrace("Lv %s found", lvId);
                    if (!vlmShouldExist)
                    {
                        if (SharedStorageUtils.isNeededBySharedResource(storDriverAccCtx, vlmData))
                        {
                            if (vlmData.isActive(storDriverAccCtx))
                            {
                                vlmsToDeactivate.add(vlmData);
                                errorReporter.logTrace(
                                    "Lv %s will not be deleted as it is needed by shared resource. " +
                                        "Deactivating LV instead",
                                    lvId
                                );
                            }
                            else
                            {
                                errorReporter.logTrace(
                                    "Lv %s will not be deleted as it is needed by shared resource. " +
                                        "Already deactivated",
                                    lvId
                                );
                            }
                        }
                        else
                        {
                            errorReporter.logTrace("Lv %s will be deleted", lvId);
                            vlmsToDelete.add(vlmData);
                        }
                    }
                    else
                    if (!vlmShouldBeActive)
                    {
                        if (vlmData.isActive(storDriverAccCtx))
                        {
                            errorReporter.logTrace("Lv %s will be deactivated", lvId);
                            vlmsToDeactivate.add(vlmData);
                        }
                        else
                        {
                            errorReporter.logTrace("Lv %s stays deactivated", lvId);
                        }
                    }
                    else
                    {
                        if (vlmData.isActive(storDriverAccCtx))
                        {
                            Size sizeState = vlmData.getSizeState();
                            if (
                                sizeState.equals(VlmProviderObject.Size.TOO_SMALL) ||
                                    ((Volume) vlmData.getVolume()).getFlags().isSet(storDriverAccCtx, Volume.Flags.RESIZE) &&
                                        (sizeState.equals(VlmProviderObject.Size.TOO_LARGE)
                                    )
                            )
                            {
                                errorReporter.logTrace(
                                    "Lv %s will be resized. Expected size: %dkb, actual size: %dkb",
                                    lvId,
                                    vlmData.getExpectedSize(),
                                    vlmData.getAllocatedSize()
                                );
                                vlmsToResize.add(vlmData);
                            }
                            vlmsToCheckForRollback.add(vlmData);
                        }
                    }
                }
                else
                {
                    errorReporter.logTrace("Cloning in progress for %s", lvId);
                }
            }
            else
            {
                if (vlmShouldExist)
                {
                    if (!cloneService.isRunning(
                            vlmData.getRscLayerObject().getResourceName(),
                            vlmData.getVlmNr(),
                            vlmData.getRscLayerObject().getResourceNameSuffix()))
                    {
                        errorReporter.logTrace("Lv %s will be created", lvId);
                        vlmsToCreate.add(vlmData);
                    }
                    else
                    {
                        errorReporter.logDebug("Cloning in process for %s", lvId);
                    }
                }
                else
                {
                    errorReporter.logTrace("Lv %s should be deleted but does not exist; no-op", lvId);
                }
            }
        }

        createVolumes(vlmsToCreate, apiCallRc);
        resizeVolumes(vlmsToResize, apiCallRc);
        deleteVolumes(vlmsToDelete, apiCallRc);
        deactivateVolumes(vlmsToDeactivate, apiCallRc);

        handleRollbacks(vlmsToCheckForRollback, apiCallRc);

        // after we are done, and the resource has the INACTIVE flag, clear the devicepath again
        for (LAYER_DATA vlmData : vlmDataList)
        {
            if (vlmData.getRscLayerObject().getAbsResource().getStateFlags().isSet(
                    storDriverAccCtx,
                    Resource.Flags.INACTIVE))
            {
                setDevicePath(vlmData, null);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void processSnapshotVolumes(List<VlmProviderObject<Snapshot>> snapVlmDataListRef, ApiCallRcImpl apiCallRcRef)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        for (VlmProviderObject<Snapshot> absSnapVlmData : snapVlmDataListRef)
        {
            LAYER_SNAP_DATA snapVlmData = (LAYER_SNAP_DATA) absSnapVlmData;
            if (snapVlmData.getRscLayerObject()
                .getAbsResource()
                .getFlags()
                .isSet(storDriverAccCtx, Snapshot.Flags.DELETE))
            {
                deleteSnapshot(snapVlmData, apiCallRcRef);
            }
            else
            {
                takeSnapshots(snapVlmData, apiCallRcRef);
            }
        }
    }

    @Override
    public LocalPropsChangePojo setLocalNodeProps(Props localNodePropsRef)
        throws StorageException, AccessDeniedException
    {
        localNodeProps = localNodePropsRef;
        return null;
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

    private boolean isCloneVolume(LAYER_DATA vlmData) throws AccessDeniedException
    {
        final Resource rsc = vlmData.getRscLayerObject().getAbsResource();
        StateFlags<Volume.Flags> vlmFlags = rsc.getVolume(vlmData.getVlmNr()).getFlags();
        return vlmFlags.isSet(storDriverAccCtx, Volume.Flags.CLONING_START) &&
            !vlmFlags.isSet(storDriverAccCtx, Volume.Flags.CLONING_FINISHED);
    }

    protected boolean isCloning(LAYER_DATA vlmData) throws AccessDeniedException
    {
        final Resource rsc = vlmData.getRscLayerObject().getAbsResource();
        return rsc.getVolume(vlmData.getVlmNr()).getFlags().isSet(storDriverAccCtx, Volume.Flags.CLONING);
    }

    protected String getCloneSnapshotName(LAYER_DATA dstVlmData)
    {
        return String.format("%s%x", CLONE_PREFIX, asLvIdentifier(dstVlmData).hashCode());
    }

    protected String getCloneSnapshotNameFull(LAYER_DATA srcVlmData, LAYER_DATA dstVlmData, String separator)
    {
        final String cloneSnapshotName = getCloneSnapshotName(dstVlmData);
        final String srcId = asLvIdentifier(srcVlmData);
        return srcId + separator + cloneSnapshotName;
    }

    private void createLvWithCopy(LAYER_DATA vlmData)
        throws AccessDeniedException, StorageException
    {
        final Props rscDfnProps = vlmData.getRscLayerObject().getAbsResource()
            .getResourceDefinition().getProps(storDriverAccCtx);
        final String srcRscName = rscDfnProps.getProp(InternalApiConsts.KEY_CLONED_FROM);
        final Resource srcRsc = getResource(vlmData, srcRscName);

        createLvWithCopyImpl(vlmData, srcRsc);
        errorReporter.logInfo("Lv copy created %s/%s", vlmData.getIdentifier(), srcRscName);
    }

    private void createVolumes(
        List<LAYER_DATA> vlmsToCreate,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        for (LAYER_DATA vlmData : vlmsToCreate)
        {
            final boolean cloneVolume = isCloneVolume(vlmData);
            String sourceLvId = computeRestoreFromResourceName(vlmData);
            // sourceLvId ends with "_00000"

            String sourceSnapshotName = computeRestoreFromSnapshotName(vlmData.getVolume());

            boolean snapRestore = sourceLvId != null && sourceSnapshotName != null;
            if (snapRestore)
            {
                errorReporter.logDebug("Restoring from lv: %s, snapshot: %s", sourceLvId, sourceSnapshotName);

                SnapshotName snapshotName;
                try
                {
                    snapshotName = new SnapshotName(sourceSnapshotName);
                }
                catch (InvalidNameException exc)
                {
                    throw new ImplementationError(exc);
                }

                AbsRscLayerObject<Resource> rscData = vlmData.getRscLayerObject();
                SnapshotDefinition snapshotDfn = rscData.getAbsResource().getResourceDefinition()
                    .getSnapshotDfn(storDriverAccCtx, snapshotName);

                boolean localRestore = false;
                if (snapshotDfn != null)
                {
                    // snapDfn might be null if we are restoring from a different resource
                    // that use case is local only and has nothing to do with backup shipping
                    Snapshot snap = snapshotDfn.getSnapshot(
                        storDriverAccCtx,
                        rscData.getAbsResource().getNode().getName()
                    );

                    if (snap.getFlags().isSet(storDriverAccCtx, Snapshot.Flags.BACKUP_TARGET) &&
                        !RscLayerSuffixes.shouldSuffixBeShipped(rscData.getResourceNameSuffix()))
                    {
                        /*
                         * Backup did purposely not contain this device, so we have to assume that we simply need to
                         * create an empty volume instead
                         */
                        createLvImpl(vlmData);

                        /*
                         * and make a snapshot from it to behave similarly as usually creating a snapshot of a volume
                         */
                        AbsRscLayerObject<Snapshot> snapLayerData = snap.getLayerData(storDriverAccCtx);
                        Set<AbsRscLayerObject<Snapshot>> storSnapLayerDataSet = LayerRscUtils.getRscDataByLayer(
                            snapLayerData,
                            DeviceLayerKind.STORAGE,
                            rscData.getResourceNameSuffix()::equals
                        );

                        if (storSnapLayerDataSet.size() > 1)
                        {
                            throw new ImplementationError(
                                "Unexpected count: " + storSnapLayerDataSet.size() + ". " + snap
                            );
                        }
                        if (!storSnapLayerDataSet.isEmpty())
                        {
                            AbsRscLayerObject<Snapshot> storSnapLayerData = storSnapLayerDataSet.iterator().next();
                            LAYER_SNAP_DATA snapVlmData = storSnapLayerData.getVlmProviderObject(vlmData.getVlmNr());
                            if (!snapshotExists(snapVlmData))
                            {
                                createSnapshot(vlmData, snapVlmData);
                            }
                            copySizes(vlmData, snapVlmData);
                        }
                    }
                    else
                    {
                        localRestore = true;
                    }
                }
                else
                {
                    localRestore = true;
                }
                if (localRestore)
                {
                    restoreSnapshot(sourceLvId, sourceSnapshotName, vlmData);
                }
            }
            else
            {
                final AbsRscLayerObject<Resource> absRscLayer = vlmData.getRscLayerObject();
                final ResourceDefinition rscDfn = absRscLayer.getAbsResource().getResourceDefinition();
                if (cloneVolume)
                {
                    if (!cloneService.isRunning(
                            absRscLayer.getResourceName(),
                            vlmData.getVlmNr(),
                            absRscLayer.getResourceNameSuffix()) &&
                        !rscDfn.getFlags().isSet(storDriverAccCtx, ResourceDefinition.Flags.FAILED))
                    {
                        createLvWithCopy(vlmData);
                    }
                }
                else
                {
                    createLvImpl(vlmData);
                }
            }
            vlmData.setExists(true);

            String storageName = getStorageName(vlmData);
            String lvId = asLvIdentifier(vlmData);

            // some providers cannot construct a device path in the next call and therefore return null here
            String devicePath = getDevicePath(storageName, lvId);

            if (!cloneVolume)
            {
                // those providers will most likely also skip setting the (null) devicePath.
                setDevicePath(vlmData, devicePath);
            }
            else
            {
                // volumes will be cloned in a background thread, and we don't want other layers try to read/write to it
                // while we currently dd/send/recv, it will be set after cloning finished
                setDevicePath(vlmData, null);
            }

            // however, those providers have to have a different method in getting the device path which means
            // the correct device path was already set since the "createLvImpl" or "restoreSnapshot" call.
            devicePath = vlmData.getDevicePath();

            if (devicePath == null)
            {
                if (!(isDevPathExpectedToBeNull || cloneVolume))
                {
                    throw new StorageException(
                        getClass().getSimpleName() + " failed to create local device for volume: " + vlmData.getVolume()
                    );
                }
            }
            else
            {
                postCreate(vlmData, !snapRestore && kind.hasBackingDevice());
            }

            addCreatedMsg(vlmData, apiCallRc);
        }
    }

    /**
     * Copies the volume's allocated and usable size to the snapVlm data.
     * CAUTION: that data is NOT stored. That means that data is gone / lost forever if the satellite is restarted.
     *
     * Currently this is only used by backup shipping which is aborted if the connection to the controller is lost,
     * so losing that data does not matter in this case
     */
    private void copySizes(LAYER_DATA vlmDataRef, LAYER_SNAP_DATA snapVlmDataRef) throws DatabaseException
    {
        snapVlmDataRef.setSnapshotAllocatedSize(vlmDataRef.getAllocatedSize());
        snapVlmDataRef.setSnapshotUsableSize(vlmDataRef.getUsableSize());
    }

    public void postCreate(LAYER_DATA vlmData, boolean wipeData)
        throws AccessDeniedException, StorageException, DatabaseException
    {
        StorPool storPool = vlmData.getStorPool();
        long waitTimeoutAfterCreate = getWaitTimeoutAfterCreate(storPool);
        waitUntilDeviceCreated(vlmData.getDevicePath(), waitTimeoutAfterCreate);

        if (stltConfigAccessor.useDmStats() && updateDmStats())
        {
            DmStatCommands.create(extCmdFactory.create(), vlmData.getDevicePath());
        }

        if (wipeData)
        {
            wipeHandler.quickWipe(vlmData.getDevicePath());
        }

        long allocatedSize = getAllocatedSize(vlmData);
        long minSize = kind.usesThinProvisioning() ? 0 : vlmData.getExpectedSize();
        if (allocatedSize < minSize)
        {
            throw new StorageException("Size of create volume is too low. Expected " +
                minSize + ". Actual: " + allocatedSize + ". Volume: " + vlmData);
        }

        setAllocatedSize(vlmData, allocatedSize);
        setUsableSize(vlmData, allocatedSize);
    }

    protected abstract boolean waitForSnapshotDevice();

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

    private void deactivateVolumes(List<LAYER_DATA> vlmsToDeactivate, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, StorageException, DatabaseException
    {
        for (LAYER_DATA vlmData : vlmsToDeactivate)
        {
            String lvId = asLvIdentifier(vlmData);

            if (stltConfigAccessor.useDmStats() && updateDmStats())
            {
                // although we do not DELETE the volume, the device (path) will be gone
                DmStatCommands.delete(extCmdFactory.create(), vlmData.getDevicePath());
            }

            deactivateLvImpl(vlmData, lvId);

            if (!vlmData.getVolume().getAbsResource().getStateFlags().isSet(
                storDriverAccCtx,
                Resource.Flags.DISK_REMOVING)
            )
            {
                addDeactivatedMsg(vlmData, apiCallRc);
            }
        }
    }

    private void deleteSnapshot(
        LAYER_SNAP_DATA snapVlm,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        if (!snapVlm.exists())
        {
            errorReporter.logTrace(
                "Snapshot '%s' not found. Skipping deletion.",
                snapVlm.toString()
            );
        }
        else
        {
            Snapshot snap = snapVlm.getRscLayerObject().getAbsResource();
            StateFlags<Flags> snapDfnFlags = snap.getSnapshotDefinition()
                .getFlags();
            if (snapDfnFlags.isSet(storDriverAccCtx, SnapshotDefinition.Flags.SHIPPING_ABORT))
            {
                snapShipMgr.abort(snapVlm);
                if (snapDfnFlags.isSet(storDriverAccCtx, SnapshotDefinition.Flags.BACKUP))
                {
                    AbsBackupShippingService backupShipService = backupShipMapper.getService(snapVlm);
                    if (backupShipService != null)
                    {
                        backupShipService.abort(snapVlm, false);
                    }
                }
            }

            errorReporter.logTrace("Deleting snapshot %s", snapVlm.toString());
            if (snapshotExists(snapVlm))
            {
                deleteSnapshotImpl(snapVlm);
            }
            else
            {
                errorReporter.logTrace("Snapshot '%s' already deleted", snapVlm.toString());
            }
            addSnapDeletedMsg(snapVlm, apiCallRc);
        }
    }

    private void takeSnapshots(
        LAYER_SNAP_DATA snapVlm,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        @Nullable LAYER_DATA vlmData = getVlmData(snapVlm);
        Snapshot snap = snapVlm.getVolume().getAbsResource();
        StateFlags<Snapshot.Flags> snapFlags = snap.getFlags();
        StateFlags<SnapshotDefinition.Flags> snapDfnFlags = snap.getSnapshotDefinition().getFlags();
        if (snapVlm.getVolume().getAbsResource().getTakeSnapshot(storDriverAccCtx))
        {
            if (vlmData == null && !snapFlags.isSet(storDriverAccCtx, Snapshot.Flags.BACKUP_TARGET))
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
                if (snapFlags.isSet(storDriverAccCtx, Snapshot.Flags.BACKUP_TARGET) &&
                    snapDfnFlags.isSet(storDriverAccCtx, SnapshotDefinition.Flags.SHIPPING))
                {
                    try
                    {
                        updateGrossSize(snapVlm); // this should round snapshots up to the ALLOCATION_GRANULARITY
                        // that is needed if a backup was created / uploaded with a larger ALLOCATION_GRANULARITY
                        // than we have here locally. The controller should have already re-calculated the property
                        // so we only need to apply and use it.

                        createLvForBackupIfNeeded(snapVlm);
                        waitForSnapIfNeeded(snapVlm);
                        startBackupRestore(snapVlm);
                    }
                    catch (InvalidNameException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                }
                else
                {
                    createSnapshot(vlmData, snapVlm);
                    copySizes(vlmData, snapVlm);

                    addSnapCreatedMsg(snapVlm, apiCallRc);

                    if (snapFlags.isSet(storDriverAccCtx, Snapshot.Flags.SHIPPING_TARGET) &&
                        snapDfnFlags.isSet(storDriverAccCtx, SnapshotDefinition.Flags.SHIPPING))
                    {
                        waitForSnapIfNeeded(snapVlm);
                        startReceiving(vlmData, snapVlm);
                    }
                }
            }
            else
            {
                try
                {
                    if (snapFlags.isSet(storDriverAccCtx, Snapshot.Flags.SHIPPING_SOURCE_START) &&
                        snapDfnFlags.isSet(storDriverAccCtx, SnapshotDefinition.Flags.SHIPPING))
                    {
                        startSending(snapVlm);
                    }
                    // else if (snapFlags.isSet(storDriverAccCtx, Snapshot.Flags.BACKUP_TARGET) &&
                    // snapDfnFlags.isSet(storDriverAccCtx, SnapshotDefinition.Flags.SHIPPING))
                    // {
                    // startBackupRestore(snapVlm);
                    // }
                }
                catch (InvalidNameException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
            if (snapFlags.isSet(storDriverAccCtx, Snapshot.Flags.BACKUP_SOURCE) &&
                snapDfnFlags.isSet(storDriverAccCtx, SnapshotDefinition.Flags.SHIPPING) &&
                !snapDfnFlags.isSet(storDriverAccCtx, SnapshotDefinition.Flags.SHIPPED))
            {
                try
                {
                    waitForSnapIfNeeded(snapVlm);
                    startBackupShipping(snapVlm);
                }
                catch (InvalidNameException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
        }
        else
        {
            if (snapFlags.isSet(storDriverAccCtx, Snapshot.Flags.SHIPPING_TARGET) &&
                snapDfnFlags.isSet(storDriverAccCtx, SnapshotDefinition.Flags.SHIPPING_CLEANUP))
            {
                errorReporter.logTrace("Post shipping cleanup for snapshot %s", snapVlm.toString());
                finishShipReceiving(vlmData, snapVlm);
            }

            /*
             * backupShippingService might be null if the snapshot does not have the
             * ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" + InternalApiConsts.KEY_BACKUP_TARGET_REMOTE
             * property set
             *
             * but in the cases where backupShippingService is null, the checked flags should
             * also be unset, preventing NPE
             */
            @Nullable AbsBackupShippingService backupShippingService = backupShipMapper.getService(snapVlm);
            if (snapDfnFlags.isSet(storDriverAccCtx, SnapshotDefinition.Flags.SHIPPING_ABORT) &&
                backupShippingService != null)
            {
                backupShippingService.abort(snapVlm, true);
            }
            if (snapshotExists(snapVlm) &&
                snapFlags.isSet(storDriverAccCtx, Snapshot.Flags.BACKUP_TARGET) &&
                snapDfnFlags.isSet(storDriverAccCtx, SnapshotDefinition.Flags.SHIPPING))
            {
                if (backupShippingService == null)
                {
                    throw new ImplementationError(
                        "Backup shipping service unexpectedly null for Snapshot: " + snapVlm.getRscLayerObject()
                            .getAbsResource()
                            .getVolume(snapVlm.getVlmNr())
                    );
                }
                else
                {
                    if (!backupShippingService.alreadyStarted(snapVlm) &&
                        !backupShippingService.alreadyFinished(snapVlm))
                    {
                        try
                        {
                            startBackupRestore(snapVlm);
                        }
                        catch (InvalidNameException exc)
                        {
                            throw new ImplementationError(exc);
                        }
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private @Nullable LAYER_DATA getVlmData(LAYER_SNAP_DATA snapVlmRef) throws AccessDeniedException
    {
        @Nullable LAYER_DATA ret;

        Snapshot snap = snapVlmRef.getRscLayerObject().getAbsResource();
        @Nullable Resource rsc = snap.getResourceDefinition().getResource(storDriverAccCtx, snap.getNodeName());
        if (rsc == null)
        {
            ret = null;
        }
        else
        {
            AbsRscLayerObject<Resource> rscLayerData = rsc.getLayerData(storDriverAccCtx);
            // since we are in the STORAGE layer...
            Set<AbsRscLayerObject<Resource>> storageRscLayerDataSet = LayerRscUtils.getRscDataByLayer(
                rscLayerData,
                DeviceLayerKind.STORAGE,
                snapVlmRef.getRscLayerObject().getResourceNameSuffix()::equals
            );

            if (storageRscLayerDataSet.isEmpty())
            {
                ret = null;
            }
            else if (storageRscLayerDataSet.size() > 1)
            {
                throw new ImplementationError(
                    "Unexpected number of storage data for: " + snapVlmRef.getRscLayerObject()
                        .getSuffixedResourceName() + "/" + snapVlmRef.getVlmNr()
                );
            }
            else
            {
                AbsRscLayerObject<Resource> storRscData = storageRscLayerDataSet.iterator().next();
                ret = (LAYER_DATA) storRscData.getVlmLayerObjects().get(snapVlmRef.getVlmNr());
            }
        }
        return ret;
    }

    protected void createLvForBackupIfNeeded(LAYER_SNAP_DATA snapVlm) throws StorageException
    {
        // do nothing, override if needed
    }

    private void waitForSnapIfNeeded(LAYER_SNAP_DATA snapVlm)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        if (waitForSnapshotDevice())
        {
            StorPool storPool = snapVlm.getStorPool();
            long waitTimeoutAfterCreate = getWaitTimeoutAfterCreate(storPool);

            String snapId = asSnapLvIdentifier(snapVlm);
            String storageName = getStorageName(storPool);
            String devicePath = getDevicePath(storageName, snapId);

            waitUntilDeviceCreated(devicePath, waitTimeoutAfterCreate);
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
                    .getRscDataByLayer(prevSnapLayerData, DeviceLayerKind.STORAGE);

                AbsRscLayerObject<Snapshot> currentSnapLayerData = snapVlm.getRscLayerObject();
                String curSnapNameSuffix = currentSnapLayerData.getResourceNameSuffix();

                for (AbsRscLayerObject<Snapshot> prevSnapStorageData : prevSnapStorageDataSet)
                {
                    String prevSnapNameSuffix = prevSnapStorageData.getResourceNameSuffix();
                    if (prevSnapNameSuffix.equals(curSnapNameSuffix))
                    {
                        prevSnapVlmData = prevSnapStorageData.getVlmProviderObject(snapVlm.getVlmNr());
                        break;
                    }
                }
            }
        }
        return prevSnapVlmData;
    }

    protected LAYER_SNAP_DATA getPreviousSnapvlmData(LAYER_SNAP_DATA snapVlm, Snapshot snap)
        throws InvalidKeyException, AccessDeniedException, InvalidNameException
    {
        LAYER_SNAP_DATA prevSnapVlmData = null;

        String prevSnapName = snap.getProps(storDriverAccCtx)
            .getProp(InternalApiConsts.KEY_BACKUP_LAST_SNAPSHOT, ApiConsts.NAMESPC_BACKUP_SHIPPING);
        if (prevSnapName != null)
        {
            SnapshotDefinition prevSnapDfn = snap.getResourceDefinition()
                .getSnapshotDfn(storDriverAccCtx, new SnapshotName(prevSnapName));
            if (prevSnapDfn != null)
            {
                AbsRscLayerObject<Snapshot> prevSnapLayerData = prevSnapDfn
                    .getSnapshot(storDriverAccCtx, snap.getNodeName())
                    .getLayerData(storDriverAccCtx);

                Set<AbsRscLayerObject<Snapshot>> prevSnapStorageDataSet = LayerRscUtils
                    .getRscDataByLayer(prevSnapLayerData, DeviceLayerKind.STORAGE);

                AbsRscLayerObject<Snapshot> currentSnapLayerData = snapVlm.getRscLayerObject();
                String curSnapNameSuffix = currentSnapLayerData.getResourceNameSuffix();

                for (AbsRscLayerObject<Snapshot> prevSnapStorageData : prevSnapStorageDataSet)
                {
                    String prevSnapNameSuffix = prevSnapStorageData.getResourceNameSuffix();
                    if (prevSnapNameSuffix.equals(curSnapNameSuffix))
                    {
                        prevSnapVlmData = prevSnapStorageData.getVlmProviderObject(snapVlm.getVlmNr());
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
        throws AccessDeniedException, StorageException
    {
        SnapshotVolume snapVlm = (SnapshotVolume) snapVlmData.getVolume();
        Props snapVlmDfnProps = snapVlm.getSnapshotVolumeDefinition().getProps(
            storDriverAccCtx
        );
        String socatPort = snapVlmDfnProps.getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_PORT);

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
        SnapshotVolumeDefinition snapVlmDfn = ((SnapshotVolume) curSnapVlmData.getVolume())
            .getSnapshotVolumeDefinition();
        Props snapDfnProps = snapDfn.getProps(storDriverAccCtx);
        Props snapVlmDfnProps = snapVlmDfn.getProps(storDriverAccCtx);

        String socatPort = snapVlmDfnProps.getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_PORT);
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

    protected void startBackupShipping(LAYER_SNAP_DATA snapVlmData)
        throws StorageException, AccessDeniedException, InvalidKeyException, InvalidNameException
    {
        SnapshotVolume snapVlm = (SnapshotVolume) snapVlmData.getVolume();
        LAYER_SNAP_DATA prevSnapVlmData = getPreviousSnapvlmData(snapVlmData, snapVlm.getAbsResource());
        backupShipMapper.getService(snapVlmData).sendBackup(
            snapVlm.getSnapshotName().displayValue,
            snapVlm.getResourceName().displayValue,
            snapVlmData.getRscLayerObject().getResourceNameSuffix(),
            snapVlm.getVolumeNumber().value,
            getSnapshotShippingSendingCommandImpl(prevSnapVlmData, snapVlmData),
            prevSnapVlmData,
            snapVlmData
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
                snapDfn.getProps(storDriverAccCtx),
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
                boolean success = false;
                try
                {
                    rollbackImpl(vlmData, rollbackTargetSnapshotName);
                    success = true;
                }
                finally
                {
                    notificationListenerProvider.get()
                        .notifySnapshotRollbackResult(
                            vlmData.getVolume().getAbsResource(),
                            apiCallRc,
                            success
                        );
                }
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
        final String msg = String.format(
            "Volume number %d of resource '%s' [%s] created",
            vlmNr,
            rscName,
            typeDescr
        );
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(ApiConsts.MASK_VLM | ApiConsts.CREATED, msg)
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName)
            .putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr))
            .build()
        );
        errorReporter.logInfo(msg);
    }

    private void addResizedMsg(LAYER_DATA vlmData, ApiCallRcImpl apiCallRc)
    {
        String rscName = vlmData.getRscLayerObject().getSuffixedResourceName();
        int vlmNr = vlmData.getVlmNr().value;
        final String msg = String.format(
            "Volume number %d of resource '%s' [%s] resized",
            vlmNr,
            rscName,
            typeDescr
        );
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(ApiConsts.MASK_VLM | ApiConsts.MODIFIED, msg)
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName)
            .putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr))
            .build()
        );
        errorReporter.logInfo(msg);
    }

    private void addDeletedMsg(LAYER_DATA vlmData, ApiCallRcImpl apiCallRc)
    {
        String rscName = vlmData.getRscLayerObject().getSuffixedResourceName();
        int vlmNr = vlmData.getVlmNr().value;
        final String msg = String.format(
            "Volume number %d of resource '%s' [%s] deleted",
            vlmNr,
            rscName,
            typeDescr
        );
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(ApiConsts.MASK_VLM | ApiConsts.DELETED, msg)
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName)
            .putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr))
            .build()
        );
        errorReporter.logInfo(msg);
    }

    private void addDeactivatedMsg(LAYER_DATA vlmData, ApiCallRcImpl apiCallRc)
    {
        String rscName = vlmData.getRscLayerObject().getSuffixedResourceName();
        int vlmNr = vlmData.getVlmNr().value;
        final String msg =                 String.format(
            "Volume number %d of resource '%s' [%s] deactivated",
            vlmNr,
            rscName,
            typeDescr
        );
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(ApiConsts.MASK_VLM | ApiConsts.MODIFIED, msg)
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName)
            .putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr))
            .build()
        );
        errorReporter.logInfo(msg);
    }

    private void addSnapCreatedMsg(LAYER_SNAP_DATA snapVlmData, ApiCallRcImpl apiCallRc)
    {
        Snapshot snap = snapVlmData.getVolume().getAbsResource();
        String snapName = snap.getSnapshotName().displayValue;
        String rscName = snap.getResourceName().displayValue;
        int vlmNr = snapVlmData.getVlmNr().value;
        final String msg = String.format(
            "Snapshot [%s] with name '%s' of resource '%s', volume number %d created.",
            typeDescr,
            snapName,
            rscName,
            vlmNr
        );
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(ApiConsts.MASK_SNAPSHOT | ApiConsts.CREATED, msg)
            .putObjRef(ApiConsts.KEY_SNAPSHOT, snapName)
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName)
            .putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr))
            .build()
        );
        errorReporter.logInfo(msg);
    }

    private void addSnapDeletedMsg(LAYER_SNAP_DATA snapVlmData, ApiCallRcImpl apiCallRc)
    {
        SnapshotVolume snapVlm = (SnapshotVolume) snapVlmData.getVolume();
        String snapName = snapVlm.getSnapshotName().displayValue;
        String rscName = snapVlm.getResourceName().displayValue;
        int vlmNr = snapVlm.getVolumeNumber().value;
        final String msg = String.format(
            "Snapshot [%s] with name '%s' of resource '%s', volume number %d deleted.",
            typeDescr,
            snapName,
            rscName,
            vlmNr
        );
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.MASK_SNAPSHOT | ApiConsts.DELETED,
                msg
            )
            .putObjRef(ApiConsts.KEY_SNAPSHOT, snapName)
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName)
            .putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr))
            .build()
        );
        errorReporter.logInfo(msg);
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
                        vlmData.getStorPool().getName(),
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

    private void startBackupRestore(LAYER_SNAP_DATA snapVlmData)
        throws StorageException, AccessDeniedException, InvalidKeyException, InvalidNameException, DatabaseException
    {
        backupShipMapper.getService(snapVlmData).restoreBackup(
            getSnapshotShippingReceivingCommandImpl(snapVlmData),
            snapVlmData
        );
    }

    private void waitUntilDeviceCreated(String devicePath, long waitTimeoutAfterCreateMillis)
        throws StorageException
    {
        if (Platform.isLinux())
        {
            DeviceUtils.waitUntilDeviceVisible(devicePath, waitTimeoutAfterCreateMillis, errorReporter, fsWatch);
        }
            /* On Windows, do nothing. Device will appear as soon as
             * resource is made primary. Also it has no representation
             * in the file system (like /dev/...).
             */
    }

    protected final @Nonnull Resource getResource(LAYER_DATA vlmData, String rscName)
        throws AccessDeniedException, StorageException
    {
        Resource rsc;
        try
        {
            ResourceName tmpName = new ResourceName(rscName);
            rsc = vlmData.getRscLayerObject().getAbsResource()
                .getNode().getResource(storDriverAccCtx, tmpName);
            if (rsc == null)
            {
                throw new StorageException("Couldn't find resource: " + rscName);
            }
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError("Couldn't create resource name from: " + rscName);
        }
        return rsc;
    }

    protected final @Nonnull LAYER_DATA getVlmDataFromResource(Resource rsc, String rscNameSuffix, VolumeNumber vlmNr)
        throws AccessDeniedException, StorageException
    {
        LAYER_DATA vlmData = null;
        List<AbsRscLayerObject<Resource>> storageRscDataList = LayerUtils.getChildLayerDataByKind(
            rsc.getLayerData(storDriverAccCtx), DeviceLayerKind.STORAGE);
        for (AbsRscLayerObject<Resource> storageRscData : storageRscDataList)
        {
            if (storageRscData.getResourceNameSuffix().equals(rscNameSuffix))
            {
                vlmData = storageRscData.getVlmProviderObject(vlmNr);
                break;
            }
        }
        if (vlmData == null)
        {
            throw new ImplementationError(
                "Couldn't find VlmData for resource: " + rsc.getResourceDefinition().getName()
            );
        }
        return vlmData;
    }

    @SuppressWarnings("unchecked")
    public void updateGrossSize(VlmProviderObject<?> vlmData)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        /*
         * First we need to round up to the next extent-size. For this we use a cache, which will be flushed when the
         * satellite restarts or gets reconnected
         */
        StorPool sp = vlmData.getStorPool();
        Long extentSizeFromSp = extentSizeFromSpCache.get(sp);
        if (extentSizeFromSp == null)
        {
            extentSizeFromSp = getExtentSize((LAYER_DATA) vlmData);
            extentSizeFromSpCache.put(sp, extentSizeFromSp);
        }
        long extentSizeFromVlmDfn = getExtentSizeFromVlmDfn(vlmData);

        long extentSize = Math.max(extentSizeFromVlmDfn, extentSizeFromSp);

        long volumeSize = vlmData.getUsableSize();

        if (volumeSize % extentSize != 0)
        {
            // round up to the next extent
            long origSize = volumeSize;
            volumeSize = ((volumeSize / extentSize) + 1) * extentSize;
            final String device = vlmData.getDevicePath() == null ?
                vlmData.getRscLayerObject().getSuffixedResourceName() + "/" + vlmData.getVlmNr().value :
                vlmData.getDevicePath();
            errorReporter.logInfo(
                String.format(
                    "Aligning %s size from %d KiB to %d KiB to be a multiple of extent size %d KiB (from %s)",
                    device,
                    origSize,
                    volumeSize,
                    extentSize,
                    extentSize == extentSizeFromSp ? "Storage Pool" : "Volume Definition"
                )
            );
        }

        // usable size was just updated (set) by the layer above us. copy that, so we can
        // update it again with the actual usable size when we are finished
        setExpectedUsableSize((LAYER_DATA) vlmData, volumeSize);
        setUsableSize((LAYER_DATA) vlmData, volumeSize);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void updateAllocatedSize(VlmProviderObject<Resource> vlmDataRef)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        LAYER_DATA vlmData = (LAYER_DATA) vlmDataRef;
        boolean isVlmActive = !vlmData.getVolume().getAbsResource().getStateFlags()
            .isSet(storDriverAccCtx, Resource.Flags.INACTIVE) &&
            ((Volume) vlmData.getVolume()).getFlags().isUnset(storDriverAccCtx, Volume.Flags.CLONING) &&
            vlmData.exists() &&
            vlmData.getDevicePath() != null;

        // if a volume is not active, there is no devicePath we can run a 'blockdev --getsize64' on...
        if (isVlmActive)
        {
            setAllocatedSize(vlmData, getAllocatedSize(vlmData));
        }
    }

    @Override
    public abstract LocalPropsChangePojo checkConfig(StorPool storPool)
        throws StorageException, AccessDeniedException;

    protected void markAllocGranAsChangedIfNeeded(
        Long extentSizeInKibRef,
        StorPool storPoolRef,
        LocalPropsChangePojo localPropsChangePojoRef
    )
    {
        if (extentSizeInKibRef != null)
        {
            String extentSizeInKibStr = extentSizeInKibRef.toString();
            try
            {
                String oldExtentSizeInKib = storPoolRef.getProps(storDriverAccCtx)
                    .getProp(
                        InternalApiConsts.ALLOCATION_GRANULARITY,
                        StorageConstants.NAMESPACE_INTERNAL
                    );
                if (!Objects.equals(extentSizeInKibStr, oldExtentSizeInKib))
                {
                    localPropsChangePojoRef.changeStorPoolProp(
                        storPoolRef,
                        StorageConstants.NAMESPACE_INTERNAL + "/" + InternalApiConsts.ALLOCATION_GRANULARITY,
                        extentSizeInKibStr
                    );
                }
            }
            catch (InvalidKeyException | AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    @Override
    public abstract SpaceInfo getSpaceInfo(StorPool storPool) throws StorageException, AccessDeniedException;

    protected abstract String getStorageName(StorPool storPoolRef) throws AccessDeniedException, StorageException;

    protected void createSnapshot(LAYER_DATA vlmData, LAYER_SNAP_DATA snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    protected void restoreSnapshot(String sourceLvId, String sourceSnapName, LAYER_DATA vlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    protected void deleteSnapshotImpl(LAYER_SNAP_DATA snapVlm)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    protected boolean snapshotExists(LAYER_SNAP_DATA snapVlm)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    protected void rollbackImpl(LAYER_DATA vlmData, String rollbackTargetSnapshotName)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    protected String getSnapshotShippingReceivingCommandImpl(LAYER_SNAP_DATA snapVlmDataRef)
        throws StorageException, AccessDeniedException
    {
        throw new StorageException("Snapshot shipping is not supported by " + getClass().getSimpleName());
    }

    protected String getSnapshotShippingSendingCommandImpl(
        LAYER_SNAP_DATA lastSnapVlmDataRef,
        LAYER_SNAP_DATA curSnapVlmDataRef
    )
        throws StorageException, AccessDeniedException
    {
        throw new StorageException("Snapshot shipping is not supported by " + getClass().getSimpleName());
    }

    protected void finishShipReceiving(LAYER_DATA vlmDataRef, LAYER_SNAP_DATA snapVlmRef)
        throws StorageException, DatabaseException, AccessDeniedException
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

    protected abstract void deactivateLvImpl(LAYER_DATA vlmData, String lvId)
        throws StorageException, AccessDeniedException, DatabaseException;

    protected void createLvWithCopyImpl(LAYER_DATA vlmData, Resource srcRsc)
        throws StorageException, AccessDeniedException
    {
        throw new StorageException("Clone volume is not supported by " + getClass().getSimpleName());
    }

    protected String asLvIdentifier(LAYER_DATA vlmData)
    {
        return asLvIdentifier(
            vlmData.getStorPool(),
            vlmData.getRscLayerObject().getResourceNameSuffix(),
            vlmData.getVolume().getVolumeDefinition()
        );
    }

    protected String asSnapLvIdentifier(LAYER_SNAP_DATA snapVlmData)
    {
        return asSnapLvIdentifier(
            snapVlmData.getStorPool(),
            snapVlmData.getRscLayerObject().getResourceNameSuffix(),
            ((SnapshotVolume) snapVlmData.getVolume()).getSnapshotVolumeDefinition()
        );
    }

    protected String asLvIdentifier(StorPool storPool, String rscNameSuffix, VolumeDefinition vlmDfn)
    {
        return getMigrationId(vlmDfn).orElse(
            asLvIdentifier(
                storPool.getName(),
                vlmDfn.getResourceDefinition().getName(),
                rscNameSuffix,
                vlmDfn.getVolumeNumber()
            )
        );
    }

    protected String asSnapLvIdentifier(StorPool storPool, String rscNameSuffix, SnapshotVolumeDefinition snapVlmDfn)
    {
        return asSnapLvIdentifier(
            storPool.getName(),
            snapVlmDfn.getResourceName(),
            rscNameSuffix,
            snapVlmDfn.getSnapshotName(),
            snapVlmDfn.getVolumeNumber()
        );
    }

    protected long getExtentSizeFromVlmDfn(VlmProviderObject<?> vlmDataRef) throws AccessDeniedException
    {
        AbsVolume<?> volume = vlmDataRef.getVolume();
        Props props;
        if (volume instanceof Volume)
        {
            VolumeDefinition vlmDfn = volume.getVolumeDefinition();
            props = vlmDfn.getProps(storDriverAccCtx);
        }
        else
        {
            SnapshotVolumeDefinition snapVlmDfn = ((SnapshotVolume) volume).getSnapshotVolumeDefinition();
            props = snapVlmDfn.getProps(storDriverAccCtx);
        }
        String allocGran = props.getProp(
            InternalApiConsts.ALLOCATION_GRANULARITY,
            StorageConstants.NAMESPACE_INTERNAL
        );
        long ret;
        if (allocGran == null)
        {
            ret = 1; // old vlmDfn, value has not yet been recalculated by controller
        }
        else
        {
            ret = Long.parseLong(allocGran);
        }
        return ret;
    }

    protected abstract String asLvIdentifier(
        StorPoolName spName,
        ResourceName resourceName,
        String rscNameSuffix,
        VolumeNumber volumeNumber
    );

    protected String asSnapLvIdentifier(
        StorPoolName spName,
        ResourceName rscName,
        String rscNameSuffix,
        SnapshotName snapName,
        VolumeNumber vlmNr
    )
    {
        return asSnapLvIdentifierRaw(
            spName.displayValue,
            rscName.displayValue,
            rscNameSuffix,
            snapName.displayValue,
            vlmNr.value
        );
    }

    protected abstract String asSnapLvIdentifierRaw(
        String spName,
        String rscName,
        String rscNameSuffix,
        String snapName,
        int vlmNr
    );

    public abstract String getDevicePath(String storageName, String lvId);

    protected abstract String getStorageName(LAYER_DATA vlmData)
        throws DatabaseException, AccessDeniedException, StorageException;

    protected abstract void setDevicePath(LAYER_DATA vlmData, String devicePath) throws DatabaseException;

    protected abstract void setAllocatedSize(LAYER_DATA vlmData, long size) throws DatabaseException;

    protected abstract void setUsableSize(LAYER_DATA vlmData, long size) throws DatabaseException;

    protected abstract void setExpectedUsableSize(LAYER_DATA vlmData, long size)
        throws DatabaseException, StorageException;

    protected abstract long getExtentSize(AbsStorageVlmData<?> vlmDataRef)
        throws StorageException, AccessDeniedException;

    public String[] getCloneCommand(CloneService.CloneInfo cloneInfo)
    {
        return null;
    }

    public void doCloneCleanup(CloneService.CloneInfo cloneInfo) throws StorageException
    {
    }
}
