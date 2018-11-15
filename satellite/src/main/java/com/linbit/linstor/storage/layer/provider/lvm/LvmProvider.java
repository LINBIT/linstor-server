package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;
import com.linbit.linstor.storage.layer.provider.StorageLayer;
import com.linbit.linstor.storage.layer.provider.lvm.LvmLayerDataStlt.Size;
import com.linbit.linstor.storage.layer.provider.utils.Commands;
import com.linbit.linstor.storage.layer.provider.utils.DmStatCommands;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.LvmUtils;
import com.linbit.linstor.storage.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.storage2.layer.data.LvmLayerData;
import com.linbit.linstor.transaction.TransactionMgr;
import javax.inject.Provider;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

public class LvmProvider implements DeviceProvider
{
    private static final int TOLERANCE_FACTOR = 3;
    private static final String FORMAT_RSC_TO_LVM_ID = "%s_%05d";
    private static final String FORMAT_LVM_ID_WIPE_IN_PROGRESS = "%s_linstor_wiping_in_progress";
    private static final String LVM_DEV_PATH_FORMAT = "/dev/%s/%s";

    protected final ExtCmdFactory extCmdFactory;
    protected final AccessContext storDriverAccCtx;
    protected final StltConfigAccessor stltConfigAccessor;
    protected final Provider<TransactionMgr> transMgrProvider;
    protected Props localNodeProps;
    private final NotificationListener notificationListener;
    private final StorageLayer storageLayer;
    private final ErrorReporter errorReporter;

    private final HashMap<String, LvsInfo> lvsCache;
    private final List<Consumer<Map<String, Long>>> postRunVolumeNotifications = new ArrayList<>();
    private final Set<String> changedVolumeGroups = new HashSet<>();

    public LvmProvider(
        ExtCmdFactory extCmdFactoryRef,
        AccessContext storDriverAccCtxRef,
        StltConfigAccessor stltConfigAccessorRef,
        Provider<TransactionMgr> transMgrProviderRef,
        NotificationListener notificationListenerRef,
        StorageLayer storageLayerRef,
        ErrorReporter errorReporterRef
    )
    {
        extCmdFactory = extCmdFactoryRef;
        storDriverAccCtx = storDriverAccCtxRef;
        stltConfigAccessor = stltConfigAccessorRef;
        transMgrProvider = transMgrProviderRef;
        notificationListener = notificationListenerRef;
        storageLayer = storageLayerRef;
        errorReporter = errorReporterRef;

        lvsCache = new HashMap<>();
    }

    @Override
    public void clearCache() throws StorageException
    {
        lvsCache.clear();

        if (!changedVolumeGroups.isEmpty())
        {
            Map<String, Long> vgFreeSizes = LvmUtils.getVgFreeSize(extCmdFactory.create(), changedVolumeGroups);
            postRunVolumeNotifications.forEach(consumer -> consumer.accept(vgFreeSizes));
        }

        changedVolumeGroups.clear();
        postRunVolumeNotifications.clear();
    }

    @Override
    public void prepare(List<Volume> volumes)
        throws StorageException, AccessDeniedException, SQLException
    {
        clearCache();

        lvsCache.putAll(
            LvmUtils.getLvsInfo(
                extCmdFactory.create(),
                getAffectedVolumeGroups(volumes)
            )
        );
        updateVolumeStates(volumes);
    }

    @Override
    public void process(List<Volume> volumes, List<SnapshotVolume> snapVolumes, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, SQLException, StorageException
    {
        List<Volume> vlmsToCreate = new ArrayList<>();
        List<Volume> vlmsToDelete = new ArrayList<>();
        List<Volume> vlmsToResize = new ArrayList<>();

        for (Volume vlm : volumes)
        {
            LvmLayerDataStlt state = (LvmLayerDataStlt) vlm.getLayerData(storDriverAccCtx);

            if (state.exists())
            {
                errorReporter.logTrace("Lv %s found", state.identifier);
                if (vlm.getFlags().isSet(storDriverAccCtx, VlmFlags.DELETE))
                {
                    errorReporter.logTrace("Lv %s will be deleted", state.identifier);
                    vlmsToDelete.add(vlm);
                }
                else
                {
                    if (
                        state.sizeState == Size.TOO_SMALL ||
                        state.sizeState == Size.TOO_LARGE // not within tolerance
                    )
                    {
                        errorReporter.logTrace("Lv %s will be resized", state.identifier);
                        vlmsToResize.add(vlm);
                    }
                }
            }
            else
            {
                if (!vlm.getFlags().isSet(storDriverAccCtx, VlmFlags.DELETE))
                {
                    errorReporter.logTrace("Lv %s will be created", state.identifier);
                    vlmsToCreate.add(vlm);
                }
                else
                {
                    errorReporter.logTrace("Lv %s should be deleted but does not exist; no-op", state.identifier);

                    String volumeGroup = getVolumeGroup(vlm);
                    addPostRunNotification(
                        volumeGroup,
                        freeSpaces ->
                            notificationListener.notifyVolumeDeleted(vlm, freeSpaces.get(volumeGroup))
                    );
                }
            }
        }

        createVolumes(vlmsToCreate, apiCallRc);
        resizeVolumes(vlmsToResize, apiCallRc);
        deleteVolumes(vlmsToDelete, apiCallRc);
    }

    private void createVolumes(List<Volume> vlms, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, SQLException
    {
        for (Volume vlm : vlms)
        {
            createLvImpl(vlm);

            String volumeGroup = ((LvmLayerData) vlm.getLayerData(storDriverAccCtx)).getVolumeGroup();
            String lvmId = asLvmIdentifier(vlm);

            vlm.setDevicePath(
                storDriverAccCtx,
                String.format(
                    LVM_DEV_PATH_FORMAT,
                    volumeGroup,
                    lvmId
                )
            );
            updateSize(vlm);

            String devicePath = vlm.getDevicePath(storDriverAccCtx);
            if (stltConfigAccessor.useDmStats())
            {
                DmStatCommands.create(extCmdFactory.create(), devicePath);
            }
            try
            {
                storageLayer.quickWipe(devicePath);
            }
            catch (IOException ioExc)
            {
                throw new StorageException("Failed to quick-wipe devicePath " + devicePath, ioExc);
            }

            addCreatedMsg(vlm, apiCallRc);
        }
    }

    /*
     * Expected to be overridden by LvmThinProvider
     */
    protected void createLvImpl(Volume vlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        LvmCommands.createFat(
            extCmdFactory.create(),
            ((LvmLayerData) vlm.getLayerData(storDriverAccCtx)).getVolumeGroup(),
            asLvmIdentifier(vlm),
            vlm.getVolumeDefinition().getVolumeSize(storDriverAccCtx)
        );
    }

    private void resizeVolumes(List<Volume> vlms, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, SQLException, StorageException
    {
        for (Volume vlm : vlms)
        {
            LvmCommands.resize(
                extCmdFactory.create(),
                ((LvmLayerData) vlm.getLayerData(storDriverAccCtx)).getVolumeGroup(),
                asLvmIdentifier(vlm),
                vlm.getVolumeDefinition().getVolumeSize(storDriverAccCtx)
            );

            updateSize(vlm);

            addResizedMsg(vlm, apiCallRc);
        }
    }

    private void deleteVolumes(List<Volume> vlms, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, SQLException
    {
        for (Volume vlm : vlms)
        {
            String lvmId = asLvmIdentifier(vlm);
            String newLvmId = String.format(FORMAT_LVM_ID_WIPE_IN_PROGRESS, UUID.randomUUID().toString());

            String devicePath = vlm.getDevicePath(storDriverAccCtx);
            // devicePath is the "current" devicePath. as we will rename it right now
            // we will have to adjust the devicePath
            int lastIndexOf = devicePath.lastIndexOf(lvmId);
            devicePath = devicePath.substring(0, lastIndexOf) + newLvmId;

            deleteLvImpl(vlm, lvmId, newLvmId, devicePath);

            if (stltConfigAccessor.useDmStats())
            {
                DmStatCommands.delete(extCmdFactory.create(), vlm.getDevicePath(storDriverAccCtx));
            }

            addDeletedMsg(vlm, apiCallRc);

            vlm.delete(storDriverAccCtx);
        }
    }

    protected void deleteLvImpl(Volume vlm, String oldLvmId, String newLvmId, String devicePath)
        throws StorageException, AccessDeniedException, SQLException
    {
        String volumeGroup = ((LvmLayerData) vlm.getLayerData(storDriverAccCtx)).getVolumeGroup();
        LvmCommands.rename(
            extCmdFactory.create(),
            volumeGroup,
            oldLvmId,
            newLvmId
        );

        storageLayer.asyncWipe(
            devicePath,
            ignored ->
            {
                try
                {
                    LvmCommands.delete(
                        extCmdFactory.create(),
                        volumeGroup,
                        newLvmId
                    );
                }
                catch (StorageException exc)
                {
                    errorReporter.reportError(exc);
                }
            }
        );
        addPostRunNotification(
            volumeGroup,
            freeSpaces ->
                notificationListener.notifyVolumeDeleted(
                    vlm,
                    freeSpaces.get(volumeGroup)
                )
        );
    }

    private void addPostRunNotification(String volumeGroup, Consumer<Map<String, Long>> consumer)
    {
        changedVolumeGroups.add(volumeGroup);
        postRunVolumeNotifications.add(consumer);
    }

    private void updateSize(Volume vlm) throws StorageException, AccessDeniedException
    {
        setSize(
            vlm,
            Commands.getBlockSizeInKib(
                extCmdFactory.create(),
                vlm.getDevicePath(storDriverAccCtx)
            )
        );
    }

    private void setSize(Volume vlm, long blockSizeInKib) throws AccessDeniedException
    {
        vlm.setAllocatedSize(storDriverAccCtx, blockSizeInKib);
        vlm.setUsableSize(storDriverAccCtx, blockSizeInKib);
    }

    /*
     * Expected to be overridden by LvmThinProvider
     */
    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException
    {
        long capacity;
        String vg = getVolumeGroup(storPool);
        if (vg == null)
        {
            throw new StorageException("Unset volume group for " + storPool);
        }
        capacity = LvmUtils.getVgTotalSize(
            extCmdFactory.create(),
            Collections.singleton(vg)
        ).get(vg);
        return capacity;
    }

    /*
     * Expected to be overridden by LvmThinProvider
     */
    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException
    {
        long freeSpace;
        String vg = getVolumeGroup(storPool);
        if (vg == null)
        {
            throw new StorageException("Unset volume group for " + storPool);
        }
        freeSpace = LvmUtils.getVgFreeSize(
            extCmdFactory.create(),
            Collections.singleton(vg)
        ).get(vg);
        return freeSpace;
    }

    /*
     * Expected to be overridden by LvmThinProvider
     */
    @Override
    public void createSnapshot(Volume vlm, String snapshotName) throws StorageException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    /*
     * Expected to be overridden by LvmThinProvider
     */
    @Override
    public void restoreSnapshot(Volume srcVlm, String snapshotName, Volume targetVlm) throws StorageException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    /*
     * Expected to be overridden by LvmThinProvider
     */
    @Override
    public void deleteSnapshot(Volume vlm, String snapshotName) throws StorageException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    /*
     * Expected to be overridden by LvmThinProvider
     */
    @Override
    public boolean snapshotExists(Volume vlm, String snapshotName) throws StorageException
    {
        throw new StorageException("Snapshots are not supported by " + getClass().getSimpleName());
    }

    /*
     * Expected to be overridden by LvmThinProvider (maybe additionally called)
     */
    @Override
    public void checkConfig(StorPool storPool) throws StorageException
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
    }

    private Set<String> getAffectedVolumeGroups(Collection<Volume> vlms)
    {
        Set<String> volumeGroups = new HashSet<>();
        try
        {
            for (Volume vlm : vlms)
            {
                volumeGroups.add(getVolumeGroup(vlm.getStorPool(storDriverAccCtx)));
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return volumeGroups;
    }

    private String getVolumeGroup(Volume vlm) throws AccessDeniedException, SQLException
    {
        String volumeGroup = null;
        LvmLayerData layerData = (LvmLayerData) vlm.getLayerData(storDriverAccCtx);
        if (layerData == null)
        {
            volumeGroup = getVolumeGroup(vlm.getStorPool(storDriverAccCtx));
        }
        else
        {
            volumeGroup = layerData.getVolumeGroup();
        }
        return volumeGroup;
    }

    protected String getVolumeGroup(StorPool storPool)
    {
        String volumeGroup;
        try
        {
            volumeGroup = DeviceLayerUtils.getNamespaceStorDriver(
                    storPool.getProps(storDriverAccCtx)
                )
                .getProp(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY);
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return volumeGroup;
    }

    private void updateVolumeStates(Collection<Volume> vlms)
        throws StorageException, AccessDeniedException, SQLException
    {
        final Map<String, Long> extentSizes = LvmUtils.getExtentSize(
            extCmdFactory.create(),
            getAffectedVolumeGroups(vlms)
        );
        for (Volume vlm : vlms)
        {
            final LvsInfo info = lvsCache.get(asLvmIdentifier(vlm));
            // final VlmStorageState<T> vlmState = vlmStorStateFactory.create((T) info, vlm);

            LvmLayerDataStlt state = (LvmLayerDataStlt) vlm.getLayerData(storDriverAccCtx);
            if (info != null)
            {
                if (state == null)
                {
                    state = createLayerData(vlm, info);
                }
                state.exists = true;

                final long expectedSize = vlm.getVolumeDefinition().getVolumeSize(storDriverAccCtx);
                final long actualSize = info.size;
                if (actualSize != expectedSize)
                {
                    if (actualSize < expectedSize)
                    {
                        state.sizeState = Size.TOO_SMALL;
                    }
                    else
                    {
                        state.sizeState = Size.TOO_LARGE;
                        final long toleratedSize =
                            expectedSize + extentSizes.get(info.volumeGroup) * TOLERANCE_FACTOR;
                        if (actualSize < toleratedSize)
                        {
                            state.sizeState = Size.TOO_LARGE_WITHIN_TOLERANCE;
                        }
                    }
                }
                vlm.setDevicePath(storDriverAccCtx, info.path);
                updateSize(vlm);
            }
            else
            {
                if (state == null)
                {
                    state = createEmptyLayerData(vlm);
                }
                state.exists = false;
                vlm.setDevicePath(storDriverAccCtx, null);
                setSize(vlm, 0);
            }
        }
    }

    /*
     * Expected to be overridden by LvmThinProvider
     */
    protected LvmLayerDataStlt createLayerData(Volume vlm, LvsInfo info) throws AccessDeniedException, SQLException
    {
        LvmLayerDataStlt data = new LvmLayerDataStlt(info);
        vlm.setLayerData(storDriverAccCtx, data);
        return data;
    }


    /*
     * Expected to be overridden by LvmThinProvider
     */
    protected LvmLayerDataStlt createEmptyLayerData(Volume vlm)
        throws AccessDeniedException, SQLException
    {
        LvmLayerDataStlt data = new LvmLayerDataStlt(
            getVolumeGroup(vlm),
            null, // thin pool
            asLvmIdentifier(vlm),
            -1
        );
        vlm.setLayerData(storDriverAccCtx, data);
        return data;
    }

    protected String asLvmIdentifier(Volume vlm)
    {
        // TODO: check for migration property
        return String.format(
            FORMAT_RSC_TO_LVM_ID,
            vlm.getResourceDefinition().getName().displayValue,
            vlm.getVolumeDefinition().getVolumeNumber().value
        );
    }

    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }

    private void addCreatedMsg(Volume vlm, ApiCallRcImpl apiCallRc)
    {
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_VLM | ApiConsts.CREATED,
                "Volume for " + vlm.getResource().toString() + " [LVM] created"
            )
        );
    }

    private void addResizedMsg(Volume vlm, ApiCallRcImpl apiCallRc)
    {
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_VLM | ApiConsts.MODIFIED,
                "Volume for " + vlm.getResource().toString() + " [LVM] resized"
            )
        );
    }

    private void addDeletedMsg(Volume vlm, ApiCallRcImpl apiCallRc)
    {
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_VLM | ApiConsts.DELETED,
                "Volume for " + vlm.getResource().toString() + " [LVM] deleted"
            )
        );
    }
}
