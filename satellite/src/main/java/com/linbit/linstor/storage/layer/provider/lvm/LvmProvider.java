package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.AbsStorageProvider;
import com.linbit.linstor.storage.layer.provider.WipeHandler;
import com.linbit.linstor.storage.layer.provider.utils.StorageConfigReader;
import com.linbit.linstor.storage.layer.provider.utils.ProviderUtils;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.LvmCommands;
import com.linbit.linstor.storage.utils.LvmUtils;
import com.linbit.linstor.storage.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.storage2.layer.data.LvmLayerData;
import com.linbit.linstor.storage2.layer.data.categories.VlmLayerData.Size;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Singleton
public class LvmProvider extends AbsStorageProvider<LvsInfo, LvmLayerDataStlt>
{
    private static final int TOLERANCE_FACTOR = 3;
    // FIXME: FORMAT should be private, only made public for LayeredSnapshotHelper
    public static final String FORMAT_RSC_TO_LVM_ID = "%s_%05d";
    private static final String FORMAT_LVM_ID_WIPE_IN_PROGRESS = "%s_linstor_wiping_in_progress";
    private static final String FORMAT_DEV_PATH = "/dev/%s/%s";

    protected LvmProvider(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        String subTypeDescr
    )
    {
        super(
            errorReporter,
            extCmdFactory,
            storDriverAccCtx,
            stltConfigAccessor,
            wipeHandler,
            notificationListenerProvider,
            subTypeDescr
        );
    }

    @Inject
    public LvmProvider(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        @DeviceManagerContext AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider
    )
    {
        super(
            errorReporter,
            extCmdFactory,
            storDriverAccCtx,
            stltConfigAccessor,
            wipeHandler,
            notificationListenerProvider,
            "LVM"
        );
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        return LvmUtils.getVgFreeSize(extCmdFactory.create(), changedStoragePoolStrings);
    }

    @Override
    protected Map<String, LvsInfo> getInfoListImpl(Collection<Volume> volumes) throws StorageException
    {
        return LvmUtils.getLvsInfo(extCmdFactory.create(), getAffectedVolumeGroups(volumes));
    }

    @Override
    protected void createLvImpl(Volume vlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        LvmCommands.createFat(
            extCmdFactory.create(),
            ((LvmLayerData) vlm.getLayerData(storDriverAccCtx)).getVolumeGroup(),
            asLvIdentifier(vlm),
            vlm.getVolumeDefinition().getVolumeSize(storDriverAccCtx)
        );
    }

    @Override
    protected void resizeLvImpl(Volume vlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        LvmCommands.resize(
            extCmdFactory.create(),
            ((LvmLayerData) vlm.getLayerData(storDriverAccCtx)).getVolumeGroup(),
            asLvIdentifier(vlm),
            vlm.getVolumeDefinition().getVolumeSize(storDriverAccCtx)
        );
    }

    @Override
    protected void deleteLvImpl(Volume vlm, String oldLvmId)
        throws StorageException, AccessDeniedException, SQLException
    {
        // just make sure to not colide with any other ongoing wipe-lv-name
        String newLvmId = String.format(FORMAT_LVM_ID_WIPE_IN_PROGRESS, UUID.randomUUID().toString());

        String devicePath = vlm.getDevicePath(storDriverAccCtx);
        // devicePath is the "current" devicePath. as we will rename it right now
        // we will have to adjust the devicePath
        int lastIndexOf = devicePath.lastIndexOf(oldLvmId);
        devicePath = devicePath.substring(0, lastIndexOf) + newLvmId;

        String volumeGroup = ((LvmLayerData) vlm.getLayerData(storDriverAccCtx)).getVolumeGroup();
        LvmCommands.rename(
            extCmdFactory.create(),
            volumeGroup,
            oldLvmId,
            newLvmId
        );

        wipeHandler.asyncWipe(
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
    }

    @Override
    protected String getDevicePath(String storageName, String lvId)
    {
        return String.format(FORMAT_DEV_PATH, storageName, lvId);
    }

    @Override
    protected String asLvIdentifier(VolumeDefinition vlmDfn)
    {
        return getMigrationId(vlmDfn).orElse(
            asLvIdentifier(
                vlmDfn.getResourceDefinition().getName(),
                vlmDfn.getVolumeNumber()
            )
        );
    }

    @Override
    protected String asLvIdentifier(ResourceName resourceName, VolumeNumber volumeNumber)
    {
        return String.format(
            FORMAT_RSC_TO_LVM_ID,
            resourceName.displayValue,
            volumeNumber.value
        );
    }

    @Override
    protected String getIdentifier(LvmLayerDataStlt layerData)
    {
        return layerData.identifier;
    }

    @Override
    protected Size getSize(LvmLayerDataStlt layerData)
    {
        return layerData.sizeState;
    }

    @Override
    protected String getStorageName(Volume vlm) throws AccessDeniedException, SQLException
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

    @Override
    protected boolean updateDmStats()
    {
        return true; // LVM driver should call dmstats commands
    }

    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException
    {
        String vg = getVolumeGroup(storPool);
        if (vg == null)
        {
            throw new StorageException("Unset volume group for " + storPool);
        }
        return LvmUtils.getVgTotalSize(
            extCmdFactory.create(),
            Collections.singleton(vg)
        ).get(vg);
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException
    {
        String vg = getVolumeGroup(storPool);
        if (vg == null)
        {
            throw new StorageException("Unset volume group for " + storPool);
        }
        return LvmUtils.getVgFreeSize(
            extCmdFactory.create(),
            Collections.singleton(vg)
        ).get(vg);
    }

    /*
     * Expected to be overridden by LvmThinProvider (maybe additionally called)
     */
    @Override
    public void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException
    {
        Props props = DeviceLayerUtils.getNamespaceStorDriver(
            storPool.getProps(storDriverAccCtx)
        );
        StorageConfigReader.checkVolumeGroupEntry(extCmdFactory.create(), props);
        StorageConfigReader.checkToleranceFactor(props);
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

    @Override
    protected void updateStates(Collection<Volume> vlms, Collection<SnapshotVolume> snapshots)
        throws StorageException, AccessDeniedException, SQLException
    {
        final Map<String, Long> extentSizes = LvmUtils.getExtentSize(
            extCmdFactory.create(),
            getAffectedVolumeGroups(vlms)
        );
        for (Volume vlm : vlms)
        {
            final LvsInfo info = infoListCache.get(asLvIdentifier(vlm));
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
                ProviderUtils.updateSize(vlm, extCmdFactory.create(), storDriverAccCtx);
            }
            else
            {
                if (state == null)
                {
                    state = createEmptyLayerData(vlm);
                }
                state.exists = false;
                vlm.setDevicePath(storDriverAccCtx, null);
                ProviderUtils.setSize(vlm, 0, storDriverAccCtx);
            }
        }

        updateSnapshotStates(snapshots);
    }

    /*
     * Expected to be overridden by LvmThinProvider
     */
    @SuppressWarnings("unused")
    protected void updateSnapshotStates(Collection<SnapshotVolume> snapshots)
        throws AccessDeniedException, SQLException
    {
        // no-op
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
            getStorageName(vlm),
            null, // thin pool
            asLvIdentifier(vlm),
            -1
        );
        vlm.setLayerData(storDriverAccCtx, data);
        return data;
    }
}
