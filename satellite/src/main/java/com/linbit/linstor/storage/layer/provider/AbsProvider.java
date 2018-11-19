package com.linbit.linstor.storage.layer.provider;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.utils.DmStatCommands;
import com.linbit.linstor.storage.layer.provider.utils.ProviderUtils;
import com.linbit.linstor.storage2.layer.data.categories.VlmLayerData;
import com.linbit.linstor.storage2.layer.data.categories.VlmLayerData.Size;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public abstract class AbsProvider<INFO, LAYER_DATA extends VlmLayerData> implements DeviceProvider
{
    protected final ErrorReporter errorReporter;
    protected final ExtCmdFactory extCmdFactory;
    protected final AccessContext storDriverAccCtx;
    protected final NotificationListener notificationListener;
    protected final StorageLayer storageLayer;
    protected final StltConfigAccessor stltConfigAccessor;
    protected Props localNodeProps;

    protected final HashMap<String, INFO> infoListCache;
    protected final List<Consumer<Map<String, Long>>> postRunVolumeNotifications = new ArrayList<>();
    protected final Set<String> changedStoragePools = new HashSet<>();
    private String typeDescr;

    public AbsProvider(
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        AccessContext storDriverAccCtxRef,
        StltConfigAccessor stltConfigAccessorRef,
        StorageLayer storageLayerRef,
        NotificationListener notificationListenerRef,
        String typeDescrRef
    )
    {
        errorReporter = errorReporterRef;
        extCmdFactory = extCmdFactoryRef;
        storDriverAccCtx = storDriverAccCtxRef;
        notificationListener = notificationListenerRef;
        storageLayer = storageLayerRef;
        stltConfigAccessor = stltConfigAccessorRef;
        typeDescr = typeDescrRef;

        infoListCache = new HashMap<>();
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
    public void prepare(List<Volume> volumes)
        throws StorageException, AccessDeniedException, SQLException
    {
        clearCache();

        infoListCache.putAll(getInfoListImpl(volumes));
        updateVolumeStates(volumes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(List<Volume> volumes, List<SnapshotVolume> snapVolumes, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, SQLException, StorageException
    {
        List<Volume> vlmsToCreate = new ArrayList<>();
        List<Volume> vlmsToDelete = new ArrayList<>();
        List<Volume> vlmsToResize = new ArrayList<>();

        for (Volume vlm : volumes)
        {
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
                        freeSpaces ->
                            notificationListener.notifyVolumeDeleted(vlm, freeSpaces.get(storageName))
                    );
                }
            }
        }

        createVolumes(vlmsToCreate, apiCallRc);
        resizeVolumes(vlmsToResize, apiCallRc);
        deleteVolumes(vlmsToDelete, apiCallRc);
    }

    @Override
    public void createSnapshot(Volume vlm, String snapshotName) throws StorageException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @Override
    public void restoreSnapshot(Volume srcVlm, String snapshotName, Volume targetVlm) throws StorageException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @Override
    public void deleteSnapshot(Volume vlm, String snapshotName) throws StorageException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @Override
    public boolean snapshotExists(Volume vlm, String snapshotName) throws StorageException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    @Override
    public abstract void checkConfig(StorPool storPool) throws StorageException;

    @Override
    public abstract long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException;

    @Override
    public abstract long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException;

    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }

    private void createVolumes(List<Volume> vlms, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, SQLException
    {
        for (Volume vlm : vlms)
        {
            createLvImpl(vlm);

            String storageName = getStorageName(vlm);
            String lvId = asLvIdentifier(vlm);

            String devicePath = getDevicePath(storageName, lvId);
            vlm.setDevicePath(storDriverAccCtx, devicePath);
            ProviderUtils.updateSize(vlm, extCmdFactory.create(), storDriverAccCtx);

            if (stltConfigAccessor.useDmStats() && updateDmStats())
            {
                DmStatCommands.create(extCmdFactory.create(), devicePath);
            }
            storageLayer.quickWipe(devicePath);

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

    protected void addPostRunNotification(String storageName, Consumer<Map<String, Long>> consumer)
    {
        changedStoragePools.add(storageName);
        postRunVolumeNotifications.add(consumer);
    }

    private void addCreatedMsg(Volume vlm, ApiCallRcImpl apiCallRc)
    {
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_VLM | ApiConsts.CREATED,
                "Volume for " + vlm.getResource().toString() + " [" + typeDescr + "] created"
            )
        );
    }

    private void addResizedMsg(Volume vlm, ApiCallRcImpl apiCallRc)
    {
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_VLM | ApiConsts.MODIFIED,
                "Volume for " + vlm.getResource().toString() + " [" + typeDescr + "] resized"
            )
        );
    }

    private void addDeletedMsg(Volume vlm, ApiCallRcImpl apiCallRc)
    {
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_VLM | ApiConsts.DELETED,
                "Volume for " + vlm.getResource().toString() + " [" + typeDescr + "] deleted"
            )
        );
    }

    protected abstract boolean updateDmStats();

    protected abstract Map<String, Long> getFreeSpacesImpl() throws StorageException;

    protected abstract Map<String, INFO> getInfoListImpl(Collection<Volume> volumes) throws StorageException;

    protected abstract void updateVolumeStates(Collection<Volume> volumes)
        throws StorageException, AccessDeniedException, SQLException;

    protected abstract void createLvImpl(Volume vlm)
        throws StorageException, AccessDeniedException, SQLException;

    protected abstract void resizeLvImpl(Volume vlm)
        throws StorageException, AccessDeniedException, SQLException;

    protected abstract void deleteLvImpl(Volume vlm, String lvId)
        throws StorageException, AccessDeniedException, SQLException;

    protected abstract String asLvIdentifier(Volume vlm);

    protected abstract String getDevicePath(String storageName, String lvId);

    protected abstract String getIdentifier(LAYER_DATA layerData);

    protected abstract Size getSize(LAYER_DATA layerData);

    protected abstract String getStorageName(Volume vlm)
        throws AccessDeniedException, SQLException;
}
