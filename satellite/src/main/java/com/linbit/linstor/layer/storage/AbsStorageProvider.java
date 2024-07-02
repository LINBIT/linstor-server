package com.linbit.linstor.layer.storage;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.Platform;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.backupshipping.AbsBackupShippingService;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.CoreModule;
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
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.drbd.DrbdInvalidateUtils;
import com.linbit.linstor.layer.storage.lvm.LvmProvider;
import com.linbit.linstor.layer.storage.utils.BlockSizeInfo;
import com.linbit.linstor.layer.storage.utils.DeviceUtils;
import com.linbit.linstor.layer.storage.utils.DmStatCommands;
import com.linbit.linstor.layer.storage.utils.SharedStorageUtils;
import com.linbit.linstor.layer.storage.utils.StltProviderUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.utils.SymbolicLinkResolver;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public abstract class AbsStorageProvider<
    INFO,
    LAYER_DATA extends AbsStorageVlmData<Resource>,
    LAYER_SNAP_DATA extends AbsStorageVlmData<Snapshot>>
    implements DeviceProvider
{

    /**
     * This class is just a simple holder class so that every class extending {@link AbsStorageProvider} can only inject
     * and forward an instance of this {@link AbsStorageProviderInit} to the abstract base class instead of having to
     * forward many other instances.
     */
    @Singleton
    public static class AbsStorageProviderInit
    {
        private final ErrorReporter errorReporter;
        private final ExtCmdFactoryStlt extCmdFactory;
        private final AccessContext storDriverAccCtx;
        private final StltConfigAccessor stltConfigAccessor;
        private final WipeHandler wipeHandler;
        private final Provider<NotificationListener> notificationListenerProvider;
        private final Provider<TransactionMgr> transMgrProvider;
        private final StltExtToolsChecker stltExtToolsChecker;
        private final CloneService cloneService;
        private final BackupShippingMgr backupShippingMgr;
        private final FileSystemWatch fileSystemWatch;
        private final CoreModule.ResourceDefinitionMap rscDfnMap;
        private final DrbdInvalidateUtils drbdInvalidateUtils;

        @Inject
        public AbsStorageProviderInit(
            ErrorReporter errorReporterRef,
            ExtCmdFactoryStlt extCmdFactoryRef,
            @DeviceManagerContext AccessContext storDriverAccCtxRef,
            StltConfigAccessor stltConfigAccessorRef,
            WipeHandler wipeHandlerRef,
            Provider<NotificationListener> notificationListenerProviderRef,
            Provider<TransactionMgr> transMgrProviderRef,
            StltExtToolsChecker stltExtToolsCheckerRef,
            CloneService cloneServiceRef,
            BackupShippingMgr backupShippingMgrRef,
            FileSystemWatch fileSystemWatchRef,
            CoreModule.ResourceDefinitionMap rscDfnMapRef,
            DrbdInvalidateUtils drbdInvalidateUtilsRef
        )
        {
            errorReporter = errorReporterRef;
            extCmdFactory = extCmdFactoryRef;
            storDriverAccCtx = storDriverAccCtxRef;
            stltConfigAccessor = stltConfigAccessorRef;
            wipeHandler = wipeHandlerRef;
            notificationListenerProvider = notificationListenerProviderRef;
            transMgrProvider = transMgrProviderRef;
            stltExtToolsChecker = stltExtToolsCheckerRef;
            cloneService = cloneServiceRef;
            backupShippingMgr = backupShippingMgrRef;
            fileSystemWatch = fileSystemWatchRef;
            rscDfnMap = rscDfnMapRef;
            drbdInvalidateUtils = drbdInvalidateUtilsRef;
        }
    }

    private static final long DFLT_WAIT_UNTIL_DEVICE_CREATED_TIMEOUT_IN_MS = 5000;
    protected static final int DFLT_STRIPES = 1;
    public static final long SIZE_OF_NOT_FOUND_STOR_POOL = ApiConsts.VAL_STOR_POOL_SPACE_NOT_FOUND;

    protected final ErrorReporter errorReporter;
    protected final ExtCmdFactoryStlt extCmdFactory;
    protected final AccessContext storDriverAccCtx;
    protected final Provider<NotificationListener> notificationListenerProvider;
    protected final Provider<TransactionMgr> transMgrProvider;
    protected final WipeHandler wipeHandler;
    protected final StltConfigAccessor stltConfigAccessor;
    protected final CoreModule.ResourceDefinitionMap rscDfnMap;
    protected @Nullable ReadOnlyProps localNodeProps;
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
    private final DrbdInvalidateUtils drbdInvalidateUtils;

    private final Map<StorPool, Long> extentSizeFromSpCache = new HashMap<>();

    private final Set<StorPool> changedStorPools = new HashSet<>();
    private boolean prepared;
    protected boolean isDevPathExpectedToBeNull = false;

    protected AbsStorageProvider(
        AbsStorageProviderInit initRef,
        String typeDescrRef,
        DeviceProviderKind kindRef
    )
    {
        errorReporter = initRef.errorReporter;
        extCmdFactory = initRef.extCmdFactory;
        storDriverAccCtx = initRef.storDriverAccCtx;
        wipeHandler = initRef.wipeHandler;
        notificationListenerProvider = initRef.notificationListenerProvider;
        stltConfigAccessor = initRef.stltConfigAccessor;
        rscDfnMap = initRef.rscDfnMap;
        transMgrProvider = initRef.transMgrProvider;
        extToolsChecker = initRef.stltExtToolsChecker;
        cloneService = initRef.cloneService;
        backupShipMapper = initRef.backupShippingMgr;
        fsWatch = initRef.fileSystemWatch;
        drbdInvalidateUtils = initRef.drbdInvalidateUtils;

        typeDescr = typeDescrRef;
        kind = kindRef;

        subclassMaintainsInfoListCache = false;
        infoListCache = new HashMap<>();
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

        List<LAYER_DATA> vlmsToSnapshotClone = new ArrayList<>();
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
                final Resource rsc = vlmData.getRscLayerObject().getAbsResource();
                var toCloneEntries = getCloneForKeyProps(rsc);
                if (!toCloneEntries.isEmpty())
                {
                    vlmsToSnapshotClone.add(vlmData);
                }
                if (!cloneService.isRunning(
                        vlmData.getRscLayerObject().getResourceName(),
                        vlmData.getVlmNr(),
                        vlmData.getRscLayerObject().getResourceNameSuffix()) || !vlmShouldExist)
                {
                    errorReporter.logTrace("Lv %s found", lvId);
                    @Nullable String parentTypeMarkedForDeletion = getParentTypeNameInDeletingState(vlmData);
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
                    if (parentTypeMarkedForDeletion != null)
                    {
                        // DO NOT try to resize. If the resize (or any other non-deleting action) failed and any of our
                        // parent types is marked for deletion, chances are that the controller is trying to first
                        // delete the diskless resources. Since we apparently are as diskful resource (since we are
                        // right now in the AbsStorageProvider), we should receive our deletion request "soon" (tm).
                        // Which means, if we would try our non-deleting action and this fails again, we would make the
                        // deletion of our diskless resource fail, preventing our own eventual deletion-operation.

                        // additionally, if we expect to be deleted soon, it should not matter whether or not we
                        // properly resize / de-/activate the LV, since it should be deleted soon anyways.

                        // if the delete flags gets removed again from all our parents, the original resize/inactivate
                        // flag should still be set, so this if here should be skipped and the original task should be
                        // continued as wanted
                        errorReporter.logTrace(
                            "Lv %s's %s is marked for deletion. Lv itself not (yet) in deleting state. Noop for now",
                            lvId,
                            parentTypeMarkedForDeletion
                        );
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
                            Volume volume = (Volume) vlmData.getVolume();
                            if (
                                sizeState.equals(VlmProviderObject.Size.TOO_SMALL) ||
                                    volume.getFlags().isSet(storDriverAccCtx, Volume.Flags.RESIZE) &&
                                    sizeState.equals(VlmProviderObject.Size.TOO_LARGE)
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

        createCloneSnapshots(vlmsToSnapshotClone, apiCallRc);
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

    private @Nullable String getParentTypeNameInDeletingState(LAYER_DATA vlmDataRef) throws AccessDeniedException
    {
        @Nullable String ret = null;
        Volume vlm = (Volume) vlmDataRef.getVolume();
        if (vlm.getFlags().isSet(storDriverAccCtx, Volume.Flags.DELETE))
        {
            ret = Volume.class.getSimpleName();
        }
        else
        {
            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
            if (vlmDfn.getFlags().isSet(storDriverAccCtx, VolumeDefinition.Flags.DELETE))
            {
                ret = VolumeDefinition.class.getSimpleName();
            }
            else
            {
                Resource rsc = vlm.getAbsResource();
                if (rsc.getStateFlags().isSet(storDriverAccCtx, Resource.Flags.DELETE))
                {
                    ret = Resource.class.getSimpleName();
                }
                else
                {
                    if (rsc.getResourceDefinition().getFlags().isSet(storDriverAccCtx, ResourceDefinition.Flags.DELETE))
                    {
                        ret = ResourceDefinition.class.getSimpleName();
                    }
                }
            }
        }
        return ret;
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
    public @Nullable LocalPropsChangePojo setLocalNodeProps(ReadOnlyProps localNodePropsRef)
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
            overrideId = snapVlmDfn.getVlmDfnProps(storDriverAccCtx).getProp(ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID);
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

    /**
     * Create a short clone snapshot name from the identifier + vlmNr.
     * @param cloneIdentifier resource name + suffix
     * @param vlmNr vlmnr of the clone
     * @return a short prefixed name and hashcode of the input parameters
     */
    public static String getCloneSnapshotName(String cloneIdentifier, int vlmNr)
    {
        final String target = String.format("%s_%d", cloneIdentifier, vlmNr);
        return String.format("%s%x", InternalApiConsts.CLONE_FOR_PREFIX, target.hashCode());
    }

    public static String getLVMCloneSnapshotNameFull(String identifier, String cloneRscName, int vlmNr)
    {
        return identifier + "_" + getCloneSnapshotName(cloneRscName, vlmNr);
    }

    public static String getZFSCloneSnapshotNameFull(String identifier, String cloneRscName, int vlmNr)
    {
        return identifier + "@" + getCloneSnapshotName(cloneRscName, vlmNr);
    }

    protected String getCloneSnapshotNameFull(LAYER_DATA srcVlmData, String cloneRscname, String separator)
    {
        final String cloneSnapshotName = getCloneSnapshotName(cloneRscname, srcVlmData.getVlmNr().value);
        final String srcId = asLvIdentifier(srcVlmData);
        return srcId + separator + cloneSnapshotName;
    }

    private void createSnapshotForClone(LAYER_DATA vlmData, String cloneRscName)
        throws AccessDeniedException, StorageException, DatabaseException
    {
        createSnapshotForCloneImpl(vlmData, cloneRscName);
        errorReporter.logInfo("Lv snapshot created %s/%s", vlmData.getIdentifier(), cloneRscName);
    }

    private Set<Map.Entry<String, String>> getCloneForKeyProps(Resource rsc) throws AccessDeniedException
    {
        final ReadOnlyProps props = rsc.getProps(storDriverAccCtx);
        return props.map().entrySet().stream()
            .filter(e -> e.getKey().startsWith(InternalApiConsts.CLONE_PROP_PREFIX))
            .collect(Collectors.toSet());
    }

    private void createCloneSnapshots(
        List<LAYER_DATA> vlmsToSnapshot,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        for (LAYER_DATA vlmData : vlmsToSnapshot)
        {
            final Resource rsc = vlmData.getRscLayerObject().getAbsResource();
            final var toCloneEntries = getCloneForKeyProps(rsc);
            for (var entry : toCloneEntries)
            {
                // do not create snapshots for suffixes that don't support cloning
                if (RscLayerSuffixes.shouldSuffixBeCloned(vlmData.getRscLayerObject().getResourceNameSuffix()))
                {
                    String cloneRscName = entry.getValue();
                    createSnapshotForClone(vlmData, cloneRscName);
                }
            }
        }
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
            @Nullable LAYER_SNAP_DATA sourceSnapVlm = getSourceSnapVlmDataForRestore(vlmData);

            boolean canRestore = false;
            if (sourceSnapVlm != null)
            {
                if (!sourceSnapVlm.exists())
                {
                    /*
                     * Using the new "rollback via restore" mechanism in combination with ZFS the following scenario may
                     * (or rather most likely) have occurred:
                     * * We created a safety_snap
                     * * We deleted the original resource
                     * * * This includes that we threw away the layer data, including the ZfsData that represented
                     * ___ sourceSnapVlm
                     * * We received the new resource containing the restore property
                     * * * We also received the resource's snapshots so we recreated the ZfsData of sourceSnapVlm, but
                     * ___ that instance has by default "exists == false"
                     * * * Our recent "zfs list" optimization only queries for the actual resource if it exists or not,
                     * ___ but does not check if the restore-source also exists. That means that sourceSnapVlm is not
                     * ___ null but still has "exists == false" although the ZFS snapshot itself also exists and is
                     * ___ ready to be used as a clone.
                     *
                     * This scenario is a simple cache-miss, which is why we run an additional check to see verify
                     * if the ZFS snapshot represented by sourceSnapVlm does indeed exist.
                     *
                     * Without this recache we would create a new (empty) ZFS volume, DrbdLayer would create new
                     * metadata but the volume would stay in Inconsistent state.
                     */
                    updateVolumeAndSnapshotStates(Collections.emptyList(), Collections.singletonList(sourceSnapVlm));
                }
                canRestore = sourceSnapVlm.exists();
            }
            if (canRestore)
            {
                errorReporter.logDebug("Restoring from snapshot: %s", asSnapLvIdentifier(sourceSnapVlm));

                boolean localRestore = false;

                StorageRscData<Snapshot> sourceSnap = sourceSnapVlm.getRscLayerObject();
                ResourceName sourceRscName = sourceSnap.getResourceName();

                StorageRscData<Resource> targetRscData = vlmData.getRscLayerObject();
                ResourceName targetRscName = targetRscData.getResourceName();
                /*
                 * With "snapshot restore" it is possible that the source and target resource names are different.
                 * With "backup restore" the resource names must be the same by design.
                 */
                if (targetRscName.equals(sourceRscName))
                {
                    Snapshot snap = sourceSnap.getAbsResource();
                    if (snap.getFlags().isSet(storDriverAccCtx, Snapshot.Flags.BACKUP_TARGET) &&
                        !RscLayerSuffixes.shouldSuffixBeShipped(targetRscData.getResourceNameSuffix()))
                    {
                        /*
                         * Backup did purposely not contain this device, which means that we will not be able to
                         * "restore from snapshot", since we do not have a snapshot of this special device.
                         * Therefore we simply need to create an empty volume instead...
                         */
                        drbdInvalidateUtils.invalidate(vlmData);
                        createLvImpl(vlmData);

                        /*
                         * ... and make a snapshot from it to behave similarly as usually creating a snapshot of a
                         * volume
                         */
                        if (!snapshotExists(sourceSnapVlm, true))
                        {
                            createSnapshot(vlmData, sourceSnapVlm, true);
                        }
                        copySizes(vlmData, sourceSnapVlm);
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
                    restoreSnapshot(sourceSnapVlm, vlmData);
                }
            }
            else
            {
                if (!cloneVolume)
                {
                    drbdInvalidateUtils.invalidate(vlmData);
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
                postCreate(vlmData, !(sourceSnapVlm != null) && kind.hasBackingDevice());
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
        waitUntilDeviceCreated(vlmData, vlmData.getDevicePath());

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
            if (snapshotExists(snapVlm, false))
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
        /*
         * backupShippingService might be null if the snapshot does not have the
         * ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" + InternalApiConsts.KEY_BACKUP_TARGET_REMOTE
         * property set
         * but in the cases where backupShippingService is null, the checked flags should
         * also be unset, preventing NPE
         */
        @Nullable AbsBackupShippingService backupShippingService = backupShipMapper.getService(snapVlm);
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

            if (!snapshotExists(snapVlm, true))
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
                    createSnapshot(vlmData, snapVlm, true);
                    copySizes(vlmData, snapVlm);

                    addSnapCreatedMsg(snapVlm, apiCallRc);
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
            if (snapDfnFlags.isSet(storDriverAccCtx, SnapshotDefinition.Flags.SHIPPING_ABORT) &&
                backupShippingService != null)
            {
                backupShippingService.abort(snapVlm, true);
            }

            /*
             * The following if case exists for the use-case where the backup-restore was already attempted but
             * failed for some reason (maybe the port was blocked / already in use). In case of LVM we already
             * have created the snapshot, so while "takeSnapshot" is set, "snapshotExists" will also return true.
             * The "takeSnapshot == true" case however only deals with "!snapshotExists" scenarios.
             * So we will have to wait (i.e. "waste" a few devMgr cycles) so that "takeSnapshot" is unset by the
             * controller so we can (re-) try starting the backup restore.
             *
             * ZFS behaves differently since ZFS does not work if the snapshot already exists. Therefore the ZfsProvider
             * does not create a dummy-snapshot, which means that while "takeSnapshot" is true, "snapshotExists" will
             * return false in case of ZFS. So if the backup-restore attempt fails in case of ZFS, the next devMgr cycle
             * will retry the backup-restore from the "takeSnapshot && !snapshotExists" case above.
             */
            if (snapshotExists(snapVlm, false) &&
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
        // do either way, but after take-snapshot, so that daemon does already exist
        if (snapDfnFlags.isSet(storDriverAccCtx, SnapshotDefinition.Flags.PREPARE_SHIPPING_ABORT) &&
            backupShippingService != null && !backupShippingService.alreadyFinished(snapVlm))
        {
            backupShippingService.setPrepareAbort(snap);
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

    protected @Nullable LAYER_SNAP_DATA getSnapVlmData(LAYER_DATA vlmDataRef, String snapNameStrRef)
        throws AccessDeniedException
    {
        try
        {
            final SnapshotName snapName = new SnapshotName(snapNameStrRef);
            final StorageRscData<Resource> rscData = vlmDataRef.getRscLayerObject();
            final Resource rsc = rscData.getAbsResource();
            final @Nullable SnapshotDefinition snapDfn = rsc.getResourceDefinition()
                .getSnapshotDfn(storDriverAccCtx, snapName);
            @Nullable Snapshot snap = null;
            if (snapDfn != null)
            {
                snap = snapDfn.getSnapshot(storDriverAccCtx, rsc.getNode().getName());
            }
            return getSnapVlmData(vlmDataRef, snap);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError("Invalid resource name as rollback target", exc);
        }
    }

    @SuppressWarnings("unchecked")
    protected @Nullable LAYER_SNAP_DATA getSnapVlmData(LAYER_DATA vlmDataRef, @Nullable Snapshot snapRef)
        throws AccessDeniedException
    {
        final @Nullable LAYER_SNAP_DATA ret;
        if (snapRef == null)
        {
            ret = null;
        }
        else
        {
            final StorageRscData<Resource> rscData = vlmDataRef.getRscLayerObject();
            AbsRscLayerObject<Snapshot> snapLayerData = snapRef.getLayerData(storDriverAccCtx);
            // since we are in the STORAGE layer...
            Set<AbsRscLayerObject<Snapshot>> storageSnapLayerDataSet = LayerRscUtils.getRscDataByLayer(
                snapLayerData,
                DeviceLayerKind.STORAGE,
                rscData.getResourceNameSuffix()::equals
            );

            if (storageSnapLayerDataSet.isEmpty())
            {
                ret = null;
            }
            else if (storageSnapLayerDataSet.size() > 1)
            {
                throw new ImplementationError(
                    "Unexpected number of storage data for: " + rscData
                        .getSuffixedResourceName() + "/" + vlmDataRef.getVlmNr() + ", snapshot: " +
                        snapRef.getSnapshotName().displayValue
                );
            }
            else
            {
                AbsRscLayerObject<Snapshot> storSnapData = storageSnapLayerDataSet.iterator().next();
                ret = (LAYER_SNAP_DATA) storSnapData.getVlmLayerObjects().get(vlmDataRef.getVlmNr());
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

    private @Nullable LAYER_SNAP_DATA getPreviousSnapvlmData(
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

    protected @Nullable LAYER_SNAP_DATA getPreviousSnapvlmData(LAYER_SNAP_DATA snapVlm, Snapshot snap)
        throws InvalidKeyException, AccessDeniedException, InvalidNameException
    {
        LAYER_SNAP_DATA prevSnapVlmData = null;

        String prevSnapName = snap.getSnapProps(storDriverAccCtx)
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
            getBackupShippingSendingCommandImpl(prevSnapVlmData, snapVlmData),
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
                snapDfn.getSnapDfnProps(storDriverAccCtx),
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
                    @Nullable LAYER_SNAP_DATA rollbackToSnapVlmData = getSnapVlmData(
                        vlmData,
                        rollbackTargetSnapshotName
                    );
                    if (rollbackToSnapVlmData == null)
                    {
                        throw new StorageException(
                            "Could not find storage snapshot of " + vlmData.getRscLayerObject()
                                .getSuffixedResourceName() + "/" + vlmData.getVlmNr() + ", snapshot: " +
                                rollbackTargetSnapshotName
                        );
                    }
                    rollbackImpl(vlmData, rollbackToSnapVlmData);
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

    private @Nullable LAYER_SNAP_DATA getSourceSnapVlmDataForRestore(LAYER_DATA vlmDataRef)
        throws AccessDeniedException, StorageException
    {
        final @Nullable LAYER_SNAP_DATA ret;
        final ReadOnlyProps props = ((Volume) vlmDataRef.getVolume()).getProps(storDriverAccCtx);
        final @Nullable String restoreFromResourceName = props.getProp(ApiConsts.KEY_VLM_RESTORE_FROM_RESOURCE);
        final @Nullable String restoreFromSnapshotProp = props.getProp(ApiConsts.KEY_VLM_RESTORE_FROM_SNAPSHOT);
        if (restoreFromResourceName != null && restoreFromSnapshotProp != null)
        {
            ResourceName rscName;
            SnapshotName snapName;
            try
            {
                rscName = new ResourceName(restoreFromResourceName);
                snapName = new SnapshotName(restoreFromSnapshotProp);
            }
            catch (InvalidNameException exc)
            {
                throw new ImplementationError(exc);
            }
            @Nullable ResourceDefinition restoreFromRscDfn = rscDfnMap.get(rscName);
            if (restoreFromRscDfn != null)
            {
                @Nullable SnapshotDefinition restoreFromSnapDfn = restoreFromRscDfn.getSnapshotDfn(
                    storDriverAccCtx,
                    snapName
                );
                if (restoreFromSnapDfn != null)
                {
                    @Nullable Snapshot restoreFromSnap = restoreFromSnapDfn.getSnapshot(
                        storDriverAccCtx,
                        vlmDataRef.getVolume().getAbsResource().getNode().getName()
                    );
                    if (restoreFromSnap != null)
                    {
                        ret = getSnapVlmData(vlmDataRef, restoreFromSnap);
                    }
                    else
                    {
                        ret = null;
                    }
                }
                else
                {
                    ret = null;
                }
            }
            else
            {
                ret = null;
            }
        }
        else
        {
            ret = null;
        }
        return ret;
    }

    private @Nullable String computeRestoreFromSnapshotName(AbsVolume<Resource> absVlm)
        throws AccessDeniedException
    {
        String restoreSnapshotName;
        try
        {
            ReadOnlyProps props = ((Volume) absVlm).getProps(storDriverAccCtx);
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
            getBackupShippingReceivingCommandImpl(snapVlmData),
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

    /**
     * Waits until the device is created.
     * Has an extra devicePath parameter, as sometimes the clone path is used to wait.
     * @param vlmData vlmData to get the storage pool and waitTimeAfterCreate from
     * @param devicePath device path to watch for.
     * @throws AccessDeniedException
     * @throws StorageException
     */
    protected void waitUntilDeviceCreated(LAYER_DATA vlmData, String devicePath)
        throws AccessDeniedException, StorageException
    {
        StorPool storPool = vlmData.getStorPool();
        long waitTimeoutAfterCreate = getWaitTimeoutAfterCreate(storPool);
        waitUntilDeviceCreated(devicePath, waitTimeoutAfterCreate);
    }

    protected final Resource getResource(LAYER_DATA vlmData, String rscName)
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

    protected final LAYER_DATA getVlmDataFromOtherResource(LAYER_DATA vlmData, String otherRscName)
        throws AccessDeniedException, StorageException
    {
        return getVlmDataFromResource(
            getResource(vlmData, otherRscName),
            vlmData.getRscLayerObject().getResourceNameSuffix(),
            vlmData.getVlmNr()
        );
    }

    protected final LAYER_DATA getVlmDataFromResource(Resource rsc, String rscNameSuffix, VolumeNumber vlmNr)
        throws AccessDeniedException
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
        int extentSizeMulFactor = getExtentSizeMulFactor(vlmData);
        long extentSizeFromVlmDfn = getExtentSizeFromVlmDfn(vlmData);

        long extentSize = Math.max(extentSizeFromVlmDfn, extentSizeFromSp * extentSizeMulFactor);

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

    /**
     * Returns the factor by which the extent size (which is StorPool based) should be multiplied. This is necessary
     * for striped LVM for example. Returns {@value AbsStorageProvider#DFLT_STRIPES} by default (unless overridden)
     *
     * @param vlmDataRef
     *      unused in the default implementation but might be used in an extending class (like
     *      {@link LvmProvider#getExtentSizeMulFactor(VlmProviderObject)})
     */
    protected int getExtentSizeMulFactor(VlmProviderObject<?> vlmDataRef)
    {
        return DFLT_STRIPES; // by default all volumes have 1 stripes (unless this method is overridden)
    }

    /**
     * Updates the minimum I/O size of a block device.
     *
     * Attempts to determine the current minimum I/O size of a block device by examining the first volume that exists
     * in the storage pool, if there is one, and if the minimum I/O size is not set in the corresponding storage
     * pool property, or if the current storage pool property's value differs from the one that was determined as the
     * current minimum I/O size, or if the storage pool property's value is unparsable, the storage pool property
     * is set to the determined value.
     *
     * @param storPoolObj The storage pool to operate on
     * @param propsChange A LocalPropsChangePojo object to use for sending the property update to the controller
     * @throws AccessDeniedException If the access context does not allow access to a volume or properties object
     */
    public void updateMinIoSize(final StorPool storPoolObj, final LocalPropsChangePojo propsChange)
        throws AccessDeniedException
    {
        final StorPoolName storPoolObjName = storPoolObj.getName();
        errorReporter.logDebug("ENTER updateMinIoSize method: Storage pool \"%s\"", storPoolObjName.displayValue);
        Collection<VlmProviderObject<Resource>> vlmProviderList = storPoolObj.getVolumes(storDriverAccCtx);
        if (vlmProviderList != null && vlmProviderList.size() >= 1)
        {
            errorReporter.logDebug("updateMinIoSize: Have vlmProviderList with %d items", vlmProviderList.size());
            Iterator<VlmProviderObject<Resource>> vlmProviderIter = vlmProviderList.iterator();
            boolean haveInfo = false;
            while (vlmProviderIter.hasNext() && !haveInfo)
            {
                VlmProviderObject<Resource> vlmProvider = vlmProviderIter.next();
                errorReporter.logDebug("updateMinIoSize: Have vlmProvider");
                final String storDevicePath = vlmProvider.getDevicePath();
                try
                {
                    updateMinIoSizeByDevice(storPoolObj, storDevicePath, propsChange);
                    haveInfo = true;
                }
                catch (IOException ignored)
                {
                }
            }
        }
        else
        {
            errorReporter.logDebug("updateMinIoSize: Don't have vlmProviderList, using temporary volumes");
            if (this instanceof ProbeVlmStorageProvider)
            {
                final ProbeVlmStorageProvider storPrv = (ProbeVlmStorageProvider) this;
                try
                {
                    final String storDevicePath = storPrv.createTmpProbeVlm(storPoolObj);
                    try
                    {
                        updateMinIoSizeByDevice(storPoolObj, storDevicePath, propsChange);
                    }
                    catch (IOException ignored)
                    {
                    }
                }
                catch (StorageException ignored)
                {
                    errorReporter.logDebug("updateMinIoSize: Temporary volume creation failed");
                }
                finally
                {
                    try
                    {
                        storPrv.deleteTmpProbeVlm(storPoolObj);
                    }
                    catch (StorageException ignored)
                    {
                        errorReporter.logDebug("updateMinIoSize: Temporary volume deletion failed");
                    }
                }
            }
        }
        errorReporter.logDebug("EXIT updateMinIoSize method");
    }

    private void updateMinIoSizeByDevice(
        final StorPool storPoolObj,
        final @Nullable String storDevicePath,
        final LocalPropsChangePojo propsChange
    )
        throws IOException, AccessDeniedException
    {
        final StorPoolName storPoolObjName = storPoolObj.getName();
        if (storDevicePath != null)
        {
            errorReporter.logDebug("updateMinIoSize: Have storDevicePath \"%s\"", storDevicePath);
            errorReporter.logDebug(
                "updateMinIoSize: Resolving symbolic links for path \"%s\"",
                storDevicePath
            );
            final Path blockDevicePath = SymbolicLinkResolver.resolveSymLink(storDevicePath);
            errorReporter.logDebug(
                "updateMinIoSize: Block device path is \"%s\"",
                blockDevicePath.toString()
            );
            final long minIoSize = BlockSizeInfo.getBlockSize(blockDevicePath);

            boolean updateValue = true;

            final String propKey = StorageConstants.NAMESPACE_INTERNAL + '/' +
                StorageConstants.BLK_DEV_MIN_IO_SIZE;
            final Props storPoolProps = storPoolObj.getProps(storDriverAccCtx);
            final @Nullable String currentPropValue = storPoolProps.getProp(propKey);
            if (currentPropValue != null)
            {
                try
                {
                    final long currentPropMinIoSize = Long.parseLong(currentPropValue);
                    updateValue = currentPropMinIoSize != minIoSize;
                }
                catch (NumberFormatException ignored)
                {
                }
            }

            if (updateValue)
            {
                final String propValue = Long.toString(minIoSize);
                errorReporter.logDebug(
                    "Storage pool \"%s\": Set property \"%s\" = \"%s\"",
                    storPoolObjName.displayValue, propKey, propValue
                );
                propsChange.changeStorPoolProp(storPoolObj, propKey, propValue);
            }
        }
        else
        {
            errorReporter.logDebug("updateMinIoSize: storDevicePath is a null pointer", storDevicePath);
        }
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

    protected void markAllocGranAsChangedIfNeeded(
        Long extentSizeInKibRef,
        StorPoolInfo storPoolRef,
        LocalPropsChangePojo localPropsChangePojoRef
    )
    {
        if (extentSizeInKibRef != null)
        {
            String extentSizeInKibStr = extentSizeInKibRef.toString();
            try
            {
                String oldExtentSizeInKib = storPoolRef.getReadOnlyProps(storDriverAccCtx)
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
    public abstract SpaceInfo getSpaceInfo(StorPoolInfo storPool) throws StorageException, AccessDeniedException;

    protected abstract String getStorageName(StorPool storPoolRef) throws AccessDeniedException, StorageException;

    protected void createSnapshot(LAYER_DATA vlmData, LAYER_SNAP_DATA snapVlmRef, boolean readOnly)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    protected void restoreSnapshot(LAYER_SNAP_DATA sourceSnapVlmData, LAYER_DATA vlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    protected void deleteSnapshotImpl(LAYER_SNAP_DATA snapVlm)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    protected boolean snapshotExists(LAYER_SNAP_DATA snapVlm, boolean forTakeSnapshotRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    protected void rollbackImpl(LAYER_DATA vlmData, LAYER_SNAP_DATA rollbackToSnapVlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    protected String getBackupShippingReceivingCommandImpl(LAYER_SNAP_DATA snapVlmDataRef)
        throws StorageException, AccessDeniedException
    {
        throw new StorageException("Snapshot shipping is not supported by " + getClass().getSimpleName());
    }

    protected String getBackupShippingSendingCommandImpl(
        LAYER_SNAP_DATA lastSnapVlmDataRef,
        LAYER_SNAP_DATA curSnapVlmDataRef
    )
        throws StorageException, AccessDeniedException
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

    protected void createSnapshotForCloneImpl(LAYER_DATA vlmData, String cloneRscName)
        throws StorageException, AccessDeniedException, DatabaseException
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

    protected abstract String asSnapLvIdentifier(LAYER_SNAP_DATA snapVlmData) throws AccessDeniedException;

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

    protected long getExtentSizeFromVlmDfn(VlmProviderObject<?> vlmDataRef) throws AccessDeniedException
    {
        AbsVolume<?> volume = vlmDataRef.getVolume();
        ReadOnlyProps props;
        if (volume instanceof Volume)
        {
            VolumeDefinition vlmDfn = volume.getVolumeDefinition();
            props = vlmDfn.getProps(storDriverAccCtx);
        }
        else
        {
            SnapshotVolumeDefinition snapVlmDfn = ((SnapshotVolume) volume).getSnapshotVolumeDefinition();
            props = snapVlmDfn.getVlmDfnProps(storDriverAccCtx);
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

    public abstract String getDevicePath(String storageName, String lvId);

    protected abstract String getStorageName(LAYER_DATA vlmData)
        throws DatabaseException, AccessDeniedException, StorageException;

    protected abstract void setDevicePath(LAYER_DATA vlmData, @Nullable String devicePath) throws DatabaseException;

    protected abstract void setAllocatedSize(LAYER_DATA vlmData, long size) throws DatabaseException;

    protected abstract void setUsableSize(LAYER_DATA vlmData, long size) throws DatabaseException;

    protected abstract void setExpectedUsableSize(LAYER_DATA vlmData, long size)
        throws DatabaseException, StorageException;

    protected abstract long getExtentSize(AbsStorageVlmData<?> vlmDataRef)
        throws StorageException, AccessDeniedException;
}
