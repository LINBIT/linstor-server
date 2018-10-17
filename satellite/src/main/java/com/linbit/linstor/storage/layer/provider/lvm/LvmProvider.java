package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.Volume.VlmFlags;
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
import com.linbit.linstor.storage.layer.provider.utils.BatchRunner;
import com.linbit.linstor.storage.layer.provider.utils.DmStatCommands;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.LvmUtils;
import com.linbit.linstor.storage.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.storage2.layer.data.LvmLayerData;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.utils.AccessUtils;
import com.linbit.utils.Pair;

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
import java.util.TreeMap;
import java.util.stream.Collectors;

public class LvmProvider implements DeviceProvider
{
    private static final int TOLERANCE_FACTOR = 3;
    private static final String FORMAT_RSC_TO_LVM_ID = "%s_%05d";
    private static final String FORMAT_LVM_ID_WIPE_IN_PROGRESS = "%s_linstor_wiping_in_progress";

    protected final ExtCmdFactory extCmdFactory;
    protected final AccessContext storDriverAccCtx;
    protected final StltConfigAccessor stltConfigAccessor;
    protected final Provider<TransactionMgr> transMgrProvider;
    protected Props localNodeProps;
    private final NotificationListener notificationListener;
    private final StorageLayer storageLayer;
    private final ErrorReporter errorReporter;

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
    }

    @Override
    public Map<Volume, StorageException> adjust(List<Volume> volumes)
        throws StorageException
    {
        Map<Volume, StorageException> returnedExceptionMap = new TreeMap<>();

        List<Volume> vlmsToCreate = new ArrayList<>();
        List<Volume> vlmsToDelete = new ArrayList<>();
        List<Volume> vlmsToResize = new ArrayList<>();

        HashMap<String, LvsInfo> lvsInfo = LvmUtils.getLvsInfo(
            extCmdFactory.create(),
            getAffectedVolumeGroups(volumes)
        );
        try
        {
            updateVolumeStates(lvsInfo, volumes);

            List<Volume> toDeleteNotExisting = new ArrayList<>();
            for (Volume vlm : volumes)
            {
                LvmLayerDataStlt state = (LvmLayerDataStlt) vlm.getLayerData(storDriverAccCtx);

                if (state.exists())
                {
                    if (vlm.getFlags().isSet(storDriverAccCtx, VlmFlags.DELETE))
                    {
                        vlmsToDelete.add(vlm);
                    }
                    else
                    {
                        if (
                            state.sizeState == Size.TOO_SMALL ||
                            state.sizeState == Size.TOO_LARGE // not within tolerance
                        )
                        {
                            vlmsToResize.add(vlm);
                        }
                    }
                }
                else
                {
                    if (!vlm.getFlags().isSet(storDriverAccCtx, VlmFlags.DELETE))
                    {
                        vlmsToCreate.add(vlm);
                    }
                    else
                    {
                        toDeleteNotExisting.add(vlm);
                    }
                }
            }

            notifyVolumesDeleted(toDeleteNotExisting);

            if (!vlmsToCreate.isEmpty())
            {
                returnedExceptionMap.putAll(
                    createVolumes(vlmsToCreate)
                );
            }
            if (!vlmsToResize.isEmpty())
            {
                returnedExceptionMap.putAll(
                    resizeVolumes(vlmsToResize)
                );
            }
            if (!vlmsToDelete.isEmpty())
            {
                returnedExceptionMap.putAll(
                    deleteVolumes(vlmsToDelete)
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(
                "Storage access context has not enough privileges to access volume",
                exc
            );
        }
        catch (SQLException exc)
        {
            throw new ImplementationError(exc);
        }

        return returnedExceptionMap;
    }

    private Map<Volume, StorageException> createVolumes(List<Volume> vlms)
        throws StorageException, AccessDeniedException, SQLException
    {
        Map<Volume, StorageException> exceptions = BatchRunner.runBatch(
            vlms,
            vlm ->
            {
                LvmCommands.create(
                    extCmdFactory.create(),
                    ((LvmLayerData) vlm.getLayerData(storDriverAccCtx)).getVolumeGroup(),
                    asLvmIdentifier(vlm),
                    getVlmDfnSize(vlm.getVolumeDefinition())
                );
            }
        );

        updateVolumeStates(vlms);

        for (Volume vlm : vlms)
        {
            String devicePath = vlm.getDevicePath(storDriverAccCtx);
            try
            {
                if (stltConfigAccessor.useDmStats())
                {
                    DmStatCommands.create(extCmdFactory.create(), devicePath);
                }
                storageLayer.quickWipe(devicePath);
            }
            catch (IOException ioExc)
            {
                exceptions.put(vlm, new StorageException("Failed to quick-wipe devicePath " + devicePath, ioExc));
            }
        }

        return exceptions;
    }

    private Map<Volume, StorageException> resizeVolumes(List<Volume> vlms)
        throws AccessDeniedException, SQLException, StorageException
    {
        Map<Volume, StorageException> exceptions = BatchRunner.runBatch(
            vlms,
            vlm ->
                LvmCommands.resize(
                    extCmdFactory.create(),
                    ((LvmLayerData) vlm.getLayerData(storDriverAccCtx)).getVolumeGroup(),
                    asLvmIdentifier(vlm),
                    getVlmDfnSize(vlm.getVolumeDefinition())
                )
        );

        updateVolumeStates(vlms);

        return exceptions;
    }

    private Map<Volume, StorageException> deleteVolumes(List<Volume> vlms)
        throws StorageException, AccessDeniedException, SQLException
    {
        Map<String, Pair<String, String>> devicesToWipe = new TreeMap<>();
        Map<Volume, StorageException> exceptions = BatchRunner.runBatch(
            vlms,
            vlm ->
            {
                String volumeGroup = ((LvmLayerData) vlm.getLayerData(storDriverAccCtx)).getVolumeGroup();
                String lvmId = asLvmIdentifier(vlm);
                String newLvmId = String.format(FORMAT_LVM_ID_WIPE_IN_PROGRESS, lvmId);

                String devicePath = vlm.getDevicePath(storDriverAccCtx);
                // devicePath is the "current" devicePath. as we will rename it right now
                // we will have to adjust the devicePath
                int lastIndexOf = devicePath.lastIndexOf(lvmId);
                devicePath = devicePath.substring(0, lastIndexOf) + newLvmId;

                devicesToWipe.put(devicePath, new Pair<String, String>(volumeGroup, lvmId));

                LvmCommands.rename(
                    extCmdFactory.create(),
                    volumeGroup,
                    lvmId,
                    newLvmId
                );
            }
        );

        updateVolumeStates(vlms);

        notifyVolumesDeleted(
            vlms.stream()
                .filter(vlm -> !exceptions.containsKey(vlm))
                .collect(Collectors.toList())
        );

        storageLayer.wipe(
            devicesToWipe.keySet(),
            wipedDevice ->
            {
                Pair<String, String> pair = devicesToWipe.get(wipedDevice);
                String volumeGroup = pair.objA;
                String lvmId = pair.objB;
                try
                {
                    LvmCommands.delete(
                        extCmdFactory.create(),
                        volumeGroup,
                        String.format(FORMAT_LVM_ID_WIPE_IN_PROGRESS, lvmId)
                    );
                }
                catch (StorageException exc)
                {
                    errorReporter.reportError(exc);
                }
            }
        );

        if (stltConfigAccessor.useDmStats())
        {
            for (Volume vlm : vlms)
            {
                DmStatCommands.delete(extCmdFactory.create(), vlm.getDevicePath(storDriverAccCtx));
            }
        }

        vlms.forEach(vlm ->
            {
                try
                {
                    vlm.delete(storDriverAccCtx);
                }
                catch (AccessDeniedException | SQLException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
        );

        return exceptions;
    }

    private void notifyVolumesDeleted(List<Volume> vlms)
    {
        Map<Volume, Long> freeSpaces = vlms.stream().collect(
            Collectors.toMap(
                vlm -> vlm,
                vlm ->
                {
                    long ret;
                    try
                    {
                        ret = getPoolFreeSpace(vlm.getStorPool(storDriverAccCtx));
                    }
                    catch (StorageException | AccessDeniedException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                    return ret;
                }
            )
        );

        vlms.stream()
            .forEach(
                vlm ->
                notificationListener.notifyVolumeDeleted(
                    vlm,
                    freeSpaces.get(vlm)
                )
            );
    }



    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException
    {
        long capacity;
        try
        {
            String vg = getVolumeGroup(storPool);
            if (vg == null)
            {
                throw new StorageException("Unset volume group for " + storPool);
            }
            capacity = LvmUtils.getVGTotalSize(
                extCmdFactory.create(),
                Collections.singleton(vg)
            ).get(vg);
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return capacity;
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException
    {
        long freeSpace;
        try
        {
            String vg = getVolumeGroup(storPool);
            if (vg == null)
            {
                throw new StorageException("Unset volume group for " + storPool);
            }
            freeSpace = LvmUtils.getVGFreeSize(
                extCmdFactory.create(),
                Collections.singleton(vg)
            ).get(vg);
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return freeSpace;
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
                volumeGroups.add(
                    getVolumeGroup(vlm.getStorPool(storDriverAccCtx))
                );
            }
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError("Failed to access storage pool", exc);
        }
        return volumeGroups;
    }

    private String getVolumeGroup(StorPool storPool) throws AccessDeniedException, InvalidKeyException
    {
        return DeviceLayerUtils.getNamespaceStorDriver(
            storPool.getProps(storDriverAccCtx)
        )
        .getProp(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY);
    }


    private void updateVolumeStates(Collection<Volume> vlms)
        throws StorageException, AccessDeniedException, SQLException
    {
        updateVolumeStates(
            LvmUtils.getLvsInfo(
                extCmdFactory.create(),
                getAffectedVolumeGroups(vlms)
            ),
            vlms
        );
    }

    private void updateVolumeStates(
        HashMap<String, LvsInfo> lvsInfo,
        Collection<Volume> vlms
    )
        throws StorageException, AccessDeniedException, SQLException
    {
        final Map<String, Long> extentSizes = LvmUtils.getExtentSize(
            extCmdFactory.create(),
            getAffectedVolumeGroups(vlms)
        );
        for (Volume vlm : vlms)
        {
            final LvsInfo info = lvsInfo.get(asLvmIdentifier(vlm));
            // final VlmStorageState<T> vlmState = vlmStorStateFactory.create((T) info, vlm);

            LvmLayerDataStlt state = (LvmLayerDataStlt) vlm.getLayerData(storDriverAccCtx);
            if (info != null)
            {
                if (state == null)
                {
                    state = createLayerData(vlm, info);
                }
                state.exists = true;

                final long expectedSize = getVlmDfnSize(vlm.getVolumeDefinition());
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
                setUsableSize(vlm, actualSize);
                setAllocatedSizeIfMax(vlm, actualSize);
                setDevicePath(vlm, info.path);
            }
            else
            {
                if (state == null)
                {
                    state = createEmptyLayerData(vlm);
                }
                state.exists = false;
                setDevicePath(vlm, null);
                setUsableSize(vlm, 0);
            }
        }
    }

    private LvmLayerDataStlt createLayerData(Volume vlm, LvsInfo info) throws AccessDeniedException, SQLException
    {
        LvmLayerDataStlt data = new LvmLayerDataStlt(info);
        vlm.setLayerData(storDriverAccCtx, data);
        return data;
    }

    private LvmLayerDataStlt createEmptyLayerData(Volume vlm) throws AccessDeniedException, SQLException
    {
        LvmLayerDataStlt data;
        try
        {
            data = new LvmLayerDataStlt(
                getVolumeGroup(vlm.getStorPool(storDriverAccCtx)),
                null, // thin pool
                asLvmIdentifier(vlm),
                -1
            );
            vlm.setLayerData(storDriverAccCtx, data);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return data;
    }

    private String asLvmIdentifier(Volume vlm)
    {
        // TODO: check for migration property
        return String.format(
            FORMAT_RSC_TO_LVM_ID,
            vlm.getResourceDefinition().getName().displayValue,
            vlm.getVolumeDefinition().getVolumeNumber().value
        );
    }

    private long getVlmDfnSize(VolumeDefinition vlmDfn)
    {
        return AccessUtils.execPrivileged(
            () -> vlmDfn.getVolumeSize(storDriverAccCtx),
            "Given storage driver access context has not enough privileges to access volume definition"
        );
    }
    private void setUsableSize(final Volume vlm, final long size)
    {
        AccessUtils.execPrivileged(
            () -> vlm.setUsableSize(storDriverAccCtx, size),
            "Given storage driver access context has not enough privileges to set usable size of volume"
        );
    }

    private void setAllocatedSizeIfMax(Volume vlm, long actualSize)
    {
        AccessUtils.execPrivileged(
            () -> vlm.setAllocatedSize(
                storDriverAccCtx,
                Math.max(
                    vlm.getAllocatedSize(storDriverAccCtx),
                    actualSize
                )
            ),
            "Given storage driver access context has not enough privileges to set allocated size of volume"
        );
    }

    private void setDevicePath(Volume vlm, String path)
    {
        AccessUtils.execPrivileged(
            () -> vlm.setDevicePath(storDriverAccCtx, path),
            "Given storage driver access context has not enough privileges to set device path of volume"
        );
    }

    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }
}
