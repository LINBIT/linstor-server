package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
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
import java.io.File;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

public class LvmThinProvider extends LvmProvider
{
    private static final String ID_SNAP_DELIMITER = "_";
    public static final String FORMAT_SNAP_VLM_TO_LVM_ID = FORMAT_RSC_TO_LVM_ID + "_%s";


    public LvmThinProvider(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        NotificationListener notificationListener
    )
    {
        super(
            errorReporter,
            extCmdFactory,
            storDriverAccCtx,
            stltConfigAccessor,
            wipeHandler,
            notificationListener,
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
        LvmCommands.createThin(
            extCmdFactory.create(),
            lvmThinData.getVolumeGroup(),
            lvmThinData.getThinPool(),
            asLvIdentifier(vlm),
            vlm.getVolumeDefinition().getVolumeSize(storDriverAccCtx)
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
    protected void createSnapshot(Volume vlm, SnapshotVolume snapVlm)
        throws StorageException, AccessDeniedException, SQLException
    {
        // TODO Auto-generated method stub
        throw new ImplementationError("Not implemented yet");
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
    protected LvmThinLayerDataStlt createLayerData(Volume vlm, LvsInfo info) throws AccessDeniedException, SQLException
    {
        LvmThinLayerDataStlt layerData = new LvmThinLayerDataStlt(info);
        vlm.setLayerData(storDriverAccCtx, layerData);
        return layerData;
    }

    @Override
    protected LvmThinLayerDataStlt createEmptyLayerData(Volume vlm) throws AccessDeniedException, SQLException
    {
        LvmThinLayerDataStlt data;
        try
        {
            StorPool storPool = vlm.getStorPool(storDriverAccCtx);
            data = new LvmThinLayerDataStlt(
                getVolumeGroup(storPool),
                getThinPool(storPool), // thin pool
                asLvIdentifier(vlm),
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

    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException
    {
        String vgForLvs = getVolumeGroupForLvs(storPool);
        return LvmUtils.getThinTotalSize(
            extCmdFactory.create(),
            Collections.singleton(vgForLvs)
        ).get(vgForLvs);
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException
    {
        String vgForLvs = getVolumeGroupForLvs(storPool);
        return LvmUtils.getThinFreeSize(
            extCmdFactory.create(),
            Collections.singleton(vgForLvs)
        ).get(vgForLvs);
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
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return volumeGroup + File.separator + thinPool;
    }

    private String getThinPool(StorPool storPool) throws AccessDeniedException, InvalidKeyException
    {
        return storPool.getProps(storDriverAccCtx).getProp(
            StorageConstants.CONFIG_LVM_THIN_POOL_KEY,
            StorageConstants.NAMESPACE_STOR_DRIVER
        );
    }

    private String getSnapshotIdentifier(SnapshotVolume snapVlm) throws AccessDeniedException
    {
        return asLvIdentifier(
            snapVlm.getResourceDefinition().getVolumeDfn(storDriverAccCtx, snapVlm.getVolumeNumber())
        ) + ID_SNAP_DELIMITER + snapVlm.getSnapshotName().displayValue;
    }
}
