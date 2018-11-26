package com.linbit.linstor.storage.layer.provider;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.fsevent.FileObserver;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.fsevent.FileSystemWatch.Event;
import com.linbit.fsevent.FileSystemWatch.FileEntry;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.Snapshot.SnapshotFlags;
import com.linbit.linstor.SnapshotVolume;
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
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.utils.DmStatCommands;
import com.linbit.linstor.storage.layer.provider.utils.ProviderUtils;
import com.linbit.linstor.storage2.layer.data.categories.VlmLayerData;
import com.linbit.linstor.storage2.layer.data.categories.VlmLayerData.Size;
import com.linbit.utils.AccessUtils;
import com.linbit.utils.Pair;

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

public abstract class AbsStorageProvider<INFO, LAYER_DATA extends VlmLayerData> implements DeviceProvider
{
    protected static final long WAIT_UNTIL_DEVICE_CREATED_TIMEOUT_IN_MS = 500;

    protected final ErrorReporter errorReporter;
    protected final ExtCmdFactory extCmdFactory;
    protected final AccessContext storDriverAccCtx;
    protected final NotificationListener notificationListener;
    protected final WipeHandler wipeHandler;
    protected final StltConfigAccessor stltConfigAccessor;
    protected Props localNodeProps;

    protected final HashMap<String, INFO> infoListCache;
    protected final List<Consumer<Map<String, Long>>> postRunVolumeNotifications = new ArrayList<>();
    protected final Set<String> changedStoragePools = new HashSet<>();
    private final String typeDescr;
    private final FileSystemWatch fsWatch;

    public AbsStorageProvider(
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        AccessContext storDriverAccCtxRef,
        StltConfigAccessor stltConfigAccessorRef,
        WipeHandler wipeHandlerRef,
        NotificationListener notificationListenerRef,
        String typeDescrRef
    )
    {
        errorReporter = errorReporterRef;
        extCmdFactory = extCmdFactoryRef;
        storDriverAccCtx = storDriverAccCtxRef;
        wipeHandler = wipeHandlerRef;
        notificationListener = notificationListenerRef;
        stltConfigAccessor = stltConfigAccessorRef;
        typeDescr = typeDescrRef;

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
        infoListCache.clear();

        if (!changedStoragePools.isEmpty())
        {
            Map<String, Long> vgFreeSizes = getFreeSpacesImpl();
            postRunVolumeNotifications.forEach(consumer -> consumer.accept(vgFreeSizes));
        }

        changedStoragePools.clear();
        postRunVolumeNotifications.clear();
    }

    @Override
    public void prepare(List<Volume> volumes, List<SnapshotVolume> snapVlms)
        throws StorageException, AccessDeniedException, SQLException
    {
        clearCache();

        infoListCache.putAll(getInfoListImpl(volumes));
        updateStates(volumes, snapVlms);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(
        List<Volume> volumes,
        List<SnapshotVolume> snapshotVlms,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException, SQLException, StorageException
    {
        Map<Boolean, List<SnapshotVolume>> groupedSnapshotVolumesByDeletingFlag = snapshotVlms.stream()
            .collect(
                Collectors.partitioningBy(
                    snapVlm -> AccessUtils.execPrivileged(
                        () -> snapVlm.getSnapshot().getFlags().isSet(storDriverAccCtx, SnapshotFlags.DELETE)
                    )
                )
            );
        Map<Pair<ResourceName, VolumeNumber>, Volume> volumesLut = new HashMap<>();

        List<Volume> vlmsToCreate = new ArrayList<>();
        List<Volume> vlmsToDelete = new ArrayList<>();
        List<Volume> vlmsToResize = new ArrayList<>();
        List<Volume> vlmsToCheckForRollback = new ArrayList<>();

        for (Volume vlm : volumes)
        {
            volumesLut.put(
                new Pair<>(
                    vlm.getResourceDefinition().getName(),
                    vlm.getVolumeDefinition().getVolumeNumber()
                ),
                vlm
            );

            LAYER_DATA state = (LAYER_DATA) vlm.getLayerData(storDriverAccCtx);

            String lvId = getIdentifier(state);
            if (state.exists())
            {
                Size lvSize = getSize(state);
                errorReporter.logTrace("Lv %s found", lvId);
                if (vlm.getFlags().isSet(storDriverAccCtx, VlmFlags.DELETE))
                {
                    errorReporter.logTrace("Lv %s will be deleted", lvId);
                    vlmsToDelete.add(vlm);
                }
                else
                {
                    if (
                        lvSize == Size.TOO_SMALL ||
                            lvSize == Size.TOO_LARGE // not within tolerance
                    )
                    {
                        errorReporter.logTrace("Lv %s will be resized", lvId);
                        vlmsToResize.add(vlm);
                    }
                    vlmsToCheckForRollback.add(vlm);
                }
            }
            else
            {
                if (!vlm.getFlags().isSet(storDriverAccCtx, VlmFlags.DELETE))
                {
                    errorReporter.logTrace("Lv %s will be created", lvId);
                    vlmsToCreate.add(vlm);
                }
                else
                {
                    errorReporter.logTrace("Lv %s should be deleted but does not exist; no-op", lvId);

                    String storageName = getStorageName(vlm);
                    addPostRunNotification(
                        storageName,
                        freeSpaces -> notificationListener.notifyVolumeDeleted(vlm, freeSpaces.get(storageName))
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
        deleteSnapshots(groupedSnapshotVolumesByDeletingFlag.get(true), apiCallRc);

        createVolumes(
            vlmsToCreate,
            groupedSnapshotVolumesByDeletingFlag.get(false),
            apiCallRc
        );
        resizeVolumes(vlmsToResize, apiCallRc);
        deleteVolumes(vlmsToDelete, apiCallRc);

        createOrRestoreSnapshots(
            volumesLut,
            groupedSnapshotVolumesByDeletingFlag.get(false),
            apiCallRc
        );

        handleRollbacks(vlmsToCheckForRollback, apiCallRc);
    }

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

    private void createVolumes(List<Volume> vlms, List<SnapshotVolume> snapVlms, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, SQLException
    {
        for (Volume vlm : vlms)
        {
            String sourceLvId = computeRestoreFromResourceName(vlm);
            // sourceLvId ends with "_00000"

            String sourceSnapshotName = computeRestoreFromSnapshotName(vlm);

            boolean snapRestore = sourceLvId != null && sourceSnapshotName != null;
            if (snapRestore)
            {
                errorReporter.logTrace("Restoring from lv: %s, snapshot: %s", sourceLvId, sourceSnapshotName);
                restoreSnapshot(sourceLvId, sourceSnapshotName, vlm);
            }
            else
            {
                createLvImpl(vlm);
            }
            String storageName = getStorageName(vlm);
            String lvId = asLvIdentifier(vlm);

            String devicePath = getDevicePath(storageName, lvId);
            vlm.setDevicePath(storDriverAccCtx, devicePath);
            waitUntilDeviceCreated(devicePath);
            ProviderUtils.updateSize(vlm, extCmdFactory.create(), storDriverAccCtx);

            if (stltConfigAccessor.useDmStats() && updateDmStats())
            {
                DmStatCommands.create(extCmdFactory.create(), devicePath);
            }

            if (!snapRestore)
            {
                wipeHandler.quickWipe(devicePath);
            }

            addCreatedMsg(vlm, apiCallRc);
        }
    }

    private void resizeVolumes(List<Volume> vlms, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, SQLException
    {
        for (Volume vlm : vlms)
        {
            resizeLvImpl(vlm);

            ProviderUtils.updateSize(vlm, extCmdFactory.create(), storDriverAccCtx);

            addResizedMsg(vlm, apiCallRc);
        }
    }

    private void deleteVolumes(List<Volume> vlms, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, StorageException, SQLException
    {
        for (Volume vlm : vlms)
        {
            String lvId = asLvIdentifier(vlm);

            deleteLvImpl(vlm, lvId);

            if (stltConfigAccessor.useDmStats() && updateDmStats())
            {
                DmStatCommands.delete(extCmdFactory.create(), vlm.getDevicePath(storDriverAccCtx));
            }

            addDeletedMsg(vlm, apiCallRc);

            vlm.delete(storDriverAccCtx);
        }
    }

    private void deleteSnapshots(List<SnapshotVolume> snapVlms, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, SQLException
    {
        for (SnapshotVolume snapVlm : snapVlms)
        {
            errorReporter.logTrace("Deleting snapshot %s", snapVlm.toString());
            deleteSnapshot(snapVlm);
            addSnapDeletedMsg(snapVlm, apiCallRc);
            snapVlm.delete(storDriverAccCtx);
        }
    }

    private void createOrRestoreSnapshots(
        Map<Pair<ResourceName, VolumeNumber>,
        Volume> volumesLut,
        List<SnapshotVolume> snapVlms,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, AccessDeniedException, SQLException
    {
        for (SnapshotVolume snapVlm : snapVlms)
        {
            Volume vlm = volumesLut.get(new Pair<>(snapVlm.getResourceName(), snapVlm.getVolumeNumber()));
            if (vlm == null)
            {
                throw new StorageException(
                    String.format(
                        "Could not create or restore snapshot '%s' as there is no corresponding volume.",
                        snapVlm.toString()
                    )
                );
            }
            if (!snapshotExists(snapVlm))
            {
                errorReporter.logTrace("Taking snapshot %s", snapVlm.toString());
                createSnapshot(vlm, snapVlm);

                addSnapCreatedMsg(snapVlm, apiCallRc);
            }
        }
    }

    private void handleRollbacks(List<Volume> vlms, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, StorageException, SQLException
    {
        for (Volume vlm : vlms)
        {
            String rollbackTargetSnapshotName = vlm.getResource()
                .getProps(storDriverAccCtx).map()
                .get(ApiConsts.KEY_RSC_ROLLBACK_TARGET);
            if (rollbackTargetSnapshotName != null)
            {
                rollbackImpl(vlm, rollbackTargetSnapshotName);
            }
        }
    }

    protected void addPostRunNotification(String storageName, Consumer<Map<String, Long>> consumer)
    {
        changedStoragePools.add(storageName);
        postRunVolumeNotifications.add(consumer);
    }

    private void addCreatedMsg(Volume vlm, ApiCallRcImpl apiCallRc)
    {
        String rscName = vlm.getResourceDefinition().getName().displayValue;
        int vlmNr = vlm.getVolumeDefinition().getVolumeNumber().value;
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

    private void addResizedMsg(Volume vlm, ApiCallRcImpl apiCallRc)
    {
        String rscName = vlm.getResourceDefinition().getName().displayValue;
        int vlmNr = vlm.getVolumeDefinition().getVolumeNumber().value;
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

    private void addDeletedMsg(Volume vlm, ApiCallRcImpl apiCallRc)
    {
        String rscName = vlm.getResourceDefinition().getName().displayValue;
        int vlmNr = vlm.getVolumeDefinition().getVolumeNumber().value;
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

    private String computeRestoreFromResourceName(Volume vlm)
        throws AccessDeniedException, StorageException
    {
        String restoreVlmName;
        try
        {
            Props props = vlm.getProps(storDriverAccCtx);
            String restoreFromResourceName = props.getProp(ApiConsts.KEY_VLM_RESTORE_FROM_RESOURCE);

            if (restoreFromResourceName != null)
            {
                restoreVlmName = getMigrationId(vlm.getVolumeDefinition()).orElse(
                    asLvIdentifier(
                        new ResourceName(restoreFromResourceName),
                        vlm.getVolumeDefinition().getVolumeNumber()
                    )
                );
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
        catch (InvalidNameException exc)
        {
            throw new StorageException("Invalid resource name", exc);
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
                fsWatch.addFileEntry(
                    new FileEntry(
                        Paths.get(devicePath),
                        Event.CREATE,
                        fileObserver
                        )
                    );
                try
                {
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

    @Override
    public abstract void checkConfig(StorPool storPool) throws StorageException;

    @Override
    public abstract long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException;

    @Override
    public abstract long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException;

    @SuppressWarnings("unused")
    protected void createSnapshot(Volume vlm, SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @SuppressWarnings("unused")
    protected void restoreSnapshot(String sourceLvId, String sourceSnapName, Volume targetVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @SuppressWarnings("unused")
    protected void deleteSnapshot(SnapshotVolume snapVlm)
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
    protected void rollbackImpl(Volume vlm, String rollbackTargetSnapshotName)
        throws StorageException, AccessDeniedException, SQLException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    protected abstract boolean updateDmStats();

    protected abstract Map<String, Long> getFreeSpacesImpl() throws StorageException;

    protected abstract Map<String, INFO> getInfoListImpl(Collection<Volume> volumes) throws StorageException;

    protected abstract void updateStates(Collection<Volume> volumes, Collection<SnapshotVolume> snapVlms)
        throws StorageException, AccessDeniedException, SQLException;

    protected abstract void createLvImpl(Volume vlm)
        throws StorageException, AccessDeniedException, SQLException;

    protected abstract void resizeLvImpl(Volume vlm)
        throws StorageException, AccessDeniedException, SQLException;

    protected abstract void deleteLvImpl(Volume vlm, String lvId)
        throws StorageException, AccessDeniedException, SQLException;

    protected String asLvIdentifier(Volume vlm)
    {
        return asLvIdentifier(vlm.getVolumeDefinition());
    }

    protected abstract String asLvIdentifier(VolumeDefinition volumeDefinition);

    protected abstract String asLvIdentifier(ResourceName resourceName, VolumeNumber volumeNumber);

    protected abstract String getDevicePath(String storageName, String lvId);

    protected abstract String getIdentifier(LAYER_DATA layerData);

    protected abstract Size getSize(LAYER_DATA layerData);

    protected abstract String getStorageName(Volume vlm)
        throws AccessDeniedException, SQLException;

}
