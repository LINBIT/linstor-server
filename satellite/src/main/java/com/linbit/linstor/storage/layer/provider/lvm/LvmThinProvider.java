package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.WipeHandler;
import com.linbit.linstor.storage.utils.LvmCommands;
import com.linbit.linstor.storage.utils.LvmUtils;
import com.linbit.linstor.storage.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.storage2.layer.data.LvmLayerData;
import com.linbit.linstor.storage2.layer.data.LvmThinLayerData;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

@Singleton
public class LvmThinProvider extends LvmProvider
{
    private static final String ID_SNAP_DELIMITER = "_";
    public static final String FORMAT_SNAP_VLM_TO_LVM_ID = FORMAT_RSC_TO_LVM_ID + "_%s";

    @Inject
    public LvmThinProvider(
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
            "LVM-Thin"
        );
    }

    @Override
    protected void updateSnapshotStates(Collection<SnapshotVolume> snapVlms)
        throws AccessDeniedException, SQLException
    {
        for (SnapshotVolume snapVlm : snapVlms)
        {
            LvsInfo info = infoListCache.get(asFullQualifiedLvIdentifier(snapVlm));
            // final VlmStorageState<T> vlmState = vlmStorStateFactory.create((T) info, vlm);

            LvmThinLayerDataStlt state = (LvmThinLayerDataStlt) snapVlm.getLayerData(storDriverAccCtx);
            state.exists = info != null;
        }
    }

    private String asFullQualifiedLvIdentifier(SnapshotVolume snapVlm)
    {
        return String.format(
            FORMAT_SNAP_VLM_TO_LVM_ID,
            snapVlm.getResourceName().displayValue,
            snapVlm.getVolumeNumber().value,
            snapVlm.getSnapshotName().displayValue
        );
    }

    @Override
    protected void createLvImpl(Volume vlm) throws StorageException, AccessDeniedException, SQLException
    {
        LvmThinLayerData lvmThinData = (LvmThinLayerData) vlm.getLayerData(storDriverAccCtx);
        String volumeGroup = lvmThinData.getVolumeGroup();
        String lvId = asLvIdentifier(vlm);
        LvmCommands.createThin(
            extCmdFactory.create(),
            volumeGroup,
            lvmThinData.getThinPool(),
            lvId,
            vlm.getUsableSize(storDriverAccCtx)
        );
        LvmCommands.activateVolume(
            extCmdFactory.create(),
            volumeGroup,
            lvId
        );
    }

    @Override
    protected void deleteLvImpl(Volume vlm, String lvmId)
        throws StorageException, AccessDeniedException, SQLException
    {
        LvmCommands.delete(
            extCmdFactory.create(),
            ((LvmLayerData) vlm.getLayerData(storDriverAccCtx)).getVolumeGroup(),
            lvmId
        );
    }

    @Override
    protected boolean snapshotExists(SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        return ((LvmThinLayerDataStlt) snapVlm.getLayerData(storDriverAccCtx)).exists;
    }

    @Override
    protected void createSnapshot(Volume vlm, SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        LvmThinLayerData lvmThinLayerData = (LvmThinLayerData) snapVlm.getLayerData(storDriverAccCtx);
        String snapshotIdentifier = getSnapshotIdentifier(snapVlm);
        LvmCommands.createSnapshotThin(
            extCmdFactory.create(),
            lvmThinLayerData.getVolumeGroup(),
            lvmThinLayerData.getThinPool(),
            asLvIdentifier(
                snapVlm.getResourceDefinition().getVolumeDfn(
                    storDriverAccCtx,
                    snapVlm.getVolumeNumber()
                )
            ),
            snapshotIdentifier
        );
    }

    @Override
    protected void deleteSnapshot(SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        LvmCommands.delete(
            extCmdFactory.create(),
            ((LvmLayerData) snapVlm.getLayerData(storDriverAccCtx)).getVolumeGroup(),
            getSnapshotIdentifier(snapVlm)
        );
    }

    @Override
    protected void restoreSnapshot(String sourceLvId, String sourceSnapName, Volume targetVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        String storageName = getStorageName(targetVlm);
        String targetId = asLvIdentifier(targetVlm);
        LvmCommands.restoreFromSnapshot(
            extCmdFactory.create(),
            sourceLvId + "_" + sourceSnapName,
            storageName,
            targetId
        );
        LvmCommands.activateVolume(
            extCmdFactory.create(),
            storageName,
            targetId
        );
    }

    @Override
    protected void rollbackImpl(Volume vlm, String rollbackTargetSnapshotName)
        throws StorageException, AccessDeniedException, SQLException
    {
        LvmThinLayerData lvmThinLayerData = (LvmThinLayerData) vlm.getLayerData(storDriverAccCtx);

        String volumeGroup = lvmThinLayerData.getVolumeGroup();
        String thinPool = lvmThinLayerData.getThinPool();
        String baseId = asLvIdentifier(vlm.getVolumeDefinition());
        String snapshotId = getSnapshotIdentifier(baseId, rollbackTargetSnapshotName);

        LvmCommands.deactivateVolume(
            extCmdFactory.create(),
            volumeGroup,
            baseId
        );

        LvmCommands.rollbackToSnapshot(
            extCmdFactory.create(),
            volumeGroup,
            snapshotId
        );

        // --merge removes the snapshot.
        // For consistency with other backends, we wish to keep the snapshot.
        // Hence we create it again here.
        // The layers above have been stopped, so the content should be identical to the original snapshot.

        LvmCommands.createSnapshotThin(
            extCmdFactory.create(),
            volumeGroup,
            thinPool,
            baseId,
            snapshotId
        );

        LvmCommands.activateVolume(
            extCmdFactory.create(),
            volumeGroup,
            baseId
        );
    }

    @Override
    protected LvmThinLayerDataStlt createLayerData(Volume vlm, LvsInfo info) throws AccessDeniedException, SQLException
    {
        LvmThinLayerDataStlt layerData = new LvmThinLayerDataStlt(info);
        vlm.setLayerData(storDriverAccCtx, layerData);
        return layerData;
    }

    @Override
    protected LvmThinLayerDataStlt createEmptyLayerData(Volume vlm) throws AccessDeniedException, SQLException
    {
        StorPool storPool = vlm.getStorPool(storDriverAccCtx);
        LvmThinLayerDataStlt data = new LvmThinLayerDataStlt(
            getVolumeGroup(storPool),
            getThinPool(storPool), // thin pool
            asLvIdentifier(vlm)
        );
        vlm.setLayerData(storDriverAccCtx, data);
        return data;
    }

    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String vgForLvs = getVolumeGroupForLvs(storPool);
        String thinPool = getThinPool(storPool);
        return LvmUtils.getThinTotalSize(
            extCmdFactory.create(),
            Collections.singleton(vgForLvs)
        ).get(thinPool);
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String vgForLvs = getVolumeGroupForLvs(storPool);
        String thinPool = getThinPool(storPool);
        return LvmUtils.getThinFreeSize(
            extCmdFactory.create(),
            Collections.singleton(vgForLvs)
        ).get(thinPool);
    }

    private String getVolumeGroupForLvs(StorPool storPool) throws StorageException
    {
        String volumeGroup;
        String thinPool;
        try
        {
            volumeGroup = getVolumeGroup(storPool);
            if (volumeGroup == null)
            {
                throw new StorageException("Unset volume group for " + storPool);
            }
            thinPool = getThinPool(storPool);
            if (thinPool == null)
            {
                throw new StorageException("Unset thin pool for " + storPool);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return volumeGroup + File.separator + thinPool;
    }

    private String getThinPool(StorPool storPool) throws AccessDeniedException
    {
        String thinPool;
        try
        {
            thinPool = storPool.getProps(storDriverAccCtx).getProp(
                StorageConstants.CONFIG_LVM_THIN_POOL_KEY,
                StorageConstants.NAMESPACE_STOR_DRIVER
            );
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded key exception", exc);
        }
        return thinPool;
    }

    /**
     * @param snapVlm
     * @return "rscName_vlmNr" + "_" + "snapshotName"
     * @throws AccessDeniedException
     */
    private String getSnapshotIdentifier(SnapshotVolume snapVlm)
    {
        return getSnapshotIdentifier(
            asLvIdentifier(snapVlm.getSnapshotVolumeDefinition()),
            snapVlm.getSnapshotName().displayValue
        );
    }

    private String getSnapshotIdentifier(String baseId, String snapshotName)
    {
        return baseId + ID_SNAP_DELIMITER + snapshotName;
    }
}
