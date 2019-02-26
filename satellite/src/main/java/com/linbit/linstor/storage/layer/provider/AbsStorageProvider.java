package com.linbit.linstor.storage.layer.provider;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.fsevent.FileObserver;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.fsevent.FileSystemWatch.Event;
import com.linbit.fsevent.FileSystemWatch.FileEntry;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.Snapshot.SnapshotFlags;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.utils.DmStatCommands;
import com.linbit.linstor.storage.layer.provider.utils.StltProviderUtils;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.utils.AccessUtils;
import com.linbit.utils.Pair;

import javax.inject.Provider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
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

public abstract class AbsStorageProvider<INFO, LAYER_DATA extends VlmProviderObject> implements DeviceProvider
{
    protected static final long WAIT_UNTIL_DEVICE_CREATED_TIMEOUT_IN_MS = 500;
    public static final long SIZE_OF_NOT_FOUND_STOR_POOL = -1;

    protected final ErrorReporter errorReporter;
    protected final ExtCmdFactory extCmdFactory;
    protected final AccessContext storDriverAccCtx;
    protected final Provider<NotificationListener> notificationListenerProvider;
    protected final Provider<TransactionMgr> transMgrProvider;
    protected final WipeHandler wipeHandler;
    protected final StltConfigAccessor stltConfigAccessor;
    protected Props localNodeProps;

    protected final HashMap<String, INFO> infoListCache;
    protected final List<Consumer<Map<String, Long>>> postRunVolumeNotifications = new ArrayList<>();
    protected final Set<String> changedStoragePoolStrings = new HashSet<>();
    private final String typeDescr;
    private final FileSystemWatch fsWatch;
    protected final DeviceProviderKind kind;

    private final Set<StorPool> changedStorPools = new HashSet<>();

    public AbsStorageProvider(
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        AccessContext storDriverAccCtxRef,
        StltConfigAccessor stltConfigAccessorRef,
        WipeHandler wipeHandlerRef,
        Provider<NotificationListener> notificationListenerProviderRef,
        Provider<TransactionMgr> transMgrProviderRef,
        String typeDescrRef,
        DeviceProviderKind kindRef
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

        changedStoragePoolStrings.clear();
        postRunVolumeNotifications.clear();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(List<VlmProviderObject> rawVlmDataList, List<SnapshotVolume> snapVlms)
        throws StorageException, AccessDeniedException, SQLException
    {
        clearCache(false);

        List<LAYER_DATA> vlmDataList = (List<LAYER_DATA>) rawVlmDataList;

        infoListCache.putAll(getInfoListImpl(vlmDataList, snapVlms));

        updateStates(vlmDataList, snapVlms);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(
        List<VlmProviderObject> rawVlmDataList,
        List<SnapshotVolume> snapshotVlms,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException, SQLException, StorageException
    {
        List<LAYER_DATA> vlmDataList = (List<LAYER_DATA>) rawVlmDataList;

        Map<Boolean, List<SnapshotVolume>> groupedSnapshotVolumesByDeletingFlag = snapshotVlms.stream()
            .collect(
                Collectors.partitioningBy(
                    snapVlm -> AccessUtils.execPrivileged(
                        () -> snapVlm.getSnapshot().getFlags().isSet(storDriverAccCtx, SnapshotFlags.DELETE)
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

            boolean vlmShouldExist = !vlmData.getVolume().getFlags().isSet(storDriverAccCtx, VlmFlags.DELETE);
            vlmShouldExist &= !vlmData.getRscLayerObject().getResource().getStateFlags().isSet(
                storDriverAccCtx,
                Resource.RscFlags.DISK_REMOVING
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
                    if (vlmData.getVolume().getFlags().isSet(storDriverAccCtx, VlmFlags.RESIZE))
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


                    String storageName = getStorageName(vlmData);
                    addPostRunNotification(
                        storageName,
                        vlmData.getVolume().getStorPool(storDriverAccCtx),
                        freeSpaces -> notificationListenerProvider.get()
                            .notifyVolumeDeleted(vlmData.getVolume(), freeSpaces.get(storageName))
                    );
                }
            }
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
        // FIXME RAID
        deleteSnapshots("", groupedSnapshotVolumesByDeletingFlag.get(true), apiCallRc);

        createVolumes(vlmsToCreate, apiCallRc);
        resizeVolumes(vlmsToResize, apiCallRc);
        deleteVolumes(vlmsToDelete, apiCallRc);

        takeSnapshots(
            volumesLut,
            groupedSnapshotVolumesByDeletingFlag.get(false),
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
        throws StorageException, AccessDeniedException, SQLException
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

            String storageName = getStorageName(vlmData);
            String lvId = asLvIdentifier(vlmData);

            String devicePath = getDevicePath(storageName, lvId);
            setDevicePath(vlmData, devicePath);
            waitUntilDeviceCreated(devicePath);

            setAllocatedSize(vlmData, getAllocatedSize(vlmData));

            if (stltConfigAccessor.useDmStats() && updateDmStats())
            {
                DmStatCommands.create(extCmdFactory.create(), devicePath);
            }

            if (!snapRestore)
            {
                wipeHandler.quickWipe(devicePath);
            }

            changedStorPools.add(vlmData.getVolume().getStorPool(storDriverAccCtx));

            addCreatedMsg(vlmData, apiCallRc);
        }
    }

    private void resizeVolumes(List<LAYER_DATA> vlmsToResize, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, SQLException
    {
        for (LAYER_DATA vlmData : vlmsToResize)
        {
            resizeLvImpl(vlmData);

            long allocatedSize = getAllocatedSize(vlmData);
            setAllocatedSize(vlmData, allocatedSize);
            setUsableSize(vlmData, allocatedSize);

            changedStorPools.add(vlmData.getVolume().getStorPool(storDriverAccCtx));

            addResizedMsg(vlmData, apiCallRc);
        }
    }

    private void deleteVolumes(List<LAYER_DATA> vlmsToDelete, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, StorageException, SQLException
    {
        for (LAYER_DATA vlmData : vlmsToDelete)
        {
            String lvId = asLvIdentifier(vlmData);

            deleteLvImpl(vlmData, lvId);

            if (stltConfigAccessor.useDmStats() && updateDmStats())
            {
                DmStatCommands.delete(extCmdFactory.create(), vlmData.getDevicePath());
            }

            changedStorPools.add(vlmData.getVolume().getStorPool(storDriverAccCtx));

            if (!vlmData.getVolume().getResource().getStateFlags().isSet(
                storDriverAccCtx,
                Resource.RscFlags.DISK_REMOVING)
            )
            {
                addDeletedMsg(vlmData, apiCallRc);

                String storageName = getStorageName(vlmData);
                addPostRunNotification(
                    storageName,
                    vlmData.getVolume().getStorPool(storDriverAccCtx),
                    freeSpaces ->
                    {
                        notificationListenerProvider.get().notifyVolumeDeleted(
                            vlmData.getVolume(),
                            freeSpaces.get(storageName)
                        );
                    }
                );
            }
        }
    }

    private void deleteSnapshots(String rscNameSuffix, List<SnapshotVolume> snapVlms, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, SQLException
    {
        for (SnapshotVolume snapVlm : snapVlms)
        {
            errorReporter.logTrace("Deleting snapshot %s", snapVlm.toString());
            if (snapshotExists(snapVlm))
            {
                deleteSnapshot(rscNameSuffix, snapVlm);
            }
            else
            {
                errorReporter.logTrace("Snapshot '%s' already deleted", snapVlm.toString());
            }

            changedStorPools.add(snapVlm.getStorPool(storDriverAccCtx));

            addSnapDeletedMsg(snapVlm, apiCallRc);
        }
    }

    private void takeSnapshots(
        Map<Pair<String, VolumeNumber>, LAYER_DATA> vlmDataLut,
        List<SnapshotVolume> snapVlms,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, AccessDeniedException, SQLException
    {
        for (SnapshotVolume snapVlm : snapVlms)
        {
            if (snapVlm.getSnapshot().getTakeSnapshot(storDriverAccCtx))
            {
                String rscName = snapVlm.getResourceName().displayValue;
                String rscNameSuffix = ""; // FIXME: RAID

                LAYER_DATA vlmData = vlmDataLut.get(new Pair<>(rscName + rscNameSuffix, snapVlm.getVolumeNumber()));
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

                    changedStorPools.add(snapVlm.getStorPool(storDriverAccCtx));

                    addSnapCreatedMsg(snapVlm, apiCallRc);
                }
            }
        }
    }

    private void handleRollbacks(List<LAYER_DATA> vlmsToCheckForRollback, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, StorageException, SQLException
    {
        for (LAYER_DATA vlmData : vlmsToCheckForRollback)
        {
            String rollbackTargetSnapshotName = vlmData.getVolume().getResource()
                .getProps(storDriverAccCtx).map()
                .get(ApiConsts.KEY_RSC_ROLLBACK_TARGET);
            if (rollbackTargetSnapshotName != null)
            {
                rollbackImpl(vlmData, rollbackTargetSnapshotName);
                changedStorPools.add(vlmData.getVolume().getStorPool(storDriverAccCtx));
            }
        }
    }

    private long getAllocatedSize(LAYER_DATA vlmData) throws StorageException
    {
        return StltProviderUtils.getAllocatedSize(vlmData, extCmdFactory.create());
    }

    protected void addPostRunNotification(
        String storageName,
        StorPool storPool,
        Consumer<Map<String, Long>> consumer
    )
    {
        changedStoragePoolStrings.add(storageName);
        changedStorPools.add(storPool);
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

    private void addSnapCreatedMsg(SnapshotVolume snapVlm, ApiCallRcImpl apiCallRc)
    {
        String snapName = snapVlm.getSnapshotName().displayValue;
        String rscName = snapVlm.getResourceName().displayValue;
        int vlmNr = snapVlm.getVolumeNumber().value;
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

    private void addSnapDeletedMsg(SnapshotVolume snapVlm, ApiCallRcImpl apiCallRc)
    {
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
                    getMigrationId(vlmData.getVlmDfnLayerObject().getVolumeDefinition())
                    .orElse(asLvIdentifier(vlmData));
            }
            else
            {
                restoreVlmName = null;
            }
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }

        return restoreVlmName;
    }

    private String computeRestoreFromSnapshotName(Volume vlm)
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

    private void waitUntilDeviceCreated(String devicePath) throws StorageException
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
                        "Waiting until device [%s] appears",
                        devicePath
                    );
                    syncObj.wait(WAIT_UNTIL_DEVICE_CREATED_TIMEOUT_IN_MS);
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
                            WAIT_UNTIL_DEVICE_CREATED_TIMEOUT_IN_MS + "ms"
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
    public void updateGrossSize(VlmProviderObject vlmData) throws AccessDeniedException, SQLException
    {
        setUsableSize(
            (LAYER_DATA) vlmData,
            vlmData.getParentAllocatedSizeOrElse(
                () -> vlmData.getVlmDfnLayerObject().getVolumeDefinition().getVolumeSize(storDriverAccCtx)
            )
        );
    }

    @Override
    public abstract void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException;

    @Override
    public abstract long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException;

    @Override
    public abstract long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException;

    @SuppressWarnings("unused")
    protected void createSnapshot(LAYER_DATA vlmData, SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @SuppressWarnings("unused")
    protected void restoreSnapshot(String sourceLvId, String sourceSnapName, LAYER_DATA vlmData)
        throws StorageException, AccessDeniedException, SQLException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @SuppressWarnings("unused")
    protected void deleteSnapshot(String rscNameSuffix, SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @SuppressWarnings("unused")
    protected boolean snapshotExists(SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @SuppressWarnings("unused")
    protected void rollbackImpl(LAYER_DATA vlmData, String rollbackTargetSnapshotName)
        throws StorageException, AccessDeniedException, SQLException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    protected abstract boolean updateDmStats();

    protected abstract Map<String, Long> getFreeSpacesImpl() throws StorageException;

    protected abstract Map<String, INFO> getInfoListImpl(
        List<LAYER_DATA> vlmDataList,
        List<SnapshotVolume> snapVlmsRef
    )
        throws StorageException, AccessDeniedException;

    protected abstract void updateStates(List<LAYER_DATA> vlmDataList, Collection<SnapshotVolume> snapVlms)
        throws StorageException, AccessDeniedException, SQLException;

    protected abstract void createLvImpl(LAYER_DATA vlmData)
        throws StorageException, AccessDeniedException, SQLException;

    protected abstract void resizeLvImpl(LAYER_DATA vlmData)
        throws StorageException, AccessDeniedException, SQLException;

    protected abstract void deleteLvImpl(LAYER_DATA vlmData, String lvId)
        throws StorageException, AccessDeniedException, SQLException;

    protected String asLvIdentifier(LAYER_DATA vlmData)
    {
        return asLvIdentifier(
            vlmData.getRscLayerObject().getResourceNameSuffix(),
            vlmData.getVolume().getVolumeDefinition()
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

    protected abstract String getDevicePath(String storageName, String lvId);

    protected abstract String getStorageName(LAYER_DATA vlmData) throws SQLException;

    protected abstract void setDevicePath(LAYER_DATA vlmData, String devicePath) throws SQLException;

    protected abstract void setAllocatedSize(LAYER_DATA vlmData, long size) throws SQLException;

    protected abstract void setUsableSize(LAYER_DATA vlmData, long size) throws SQLException;

}
