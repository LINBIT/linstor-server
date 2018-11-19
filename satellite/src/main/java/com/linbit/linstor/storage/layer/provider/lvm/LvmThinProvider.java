package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
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
import com.linbit.linstor.storage.layer.provider.StorageLayer;
import com.linbit.linstor.storage.utils.LvmCommands;
import com.linbit.linstor.storage.utils.LvmUtils;
import com.linbit.linstor.storage.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.storage2.layer.data.LvmLayerData;
import com.linbit.linstor.storage2.layer.data.LvmThinLayerData;
import java.io.File;
import java.sql.SQLException;
import java.util.Collections;

public class LvmThinProvider extends LvmProvider
{
    public LvmThinProvider(
        ErrorReporter errorReporterRef,
        ExtCmdFactory extCmdFactoryRef,
        AccessContext storDriverAccCtxRef,
        StltConfigAccessor stltConfigAccessorRef,
        StorageLayer storageLayerRef,
        NotificationListener notificationListenerRef
    )
    {
        super(
            errorReporterRef,
            extCmdFactoryRef,
            storDriverAccCtxRef,
            stltConfigAccessorRef,
            storageLayerRef,
            notificationListenerRef,
            "LVM-Thin"
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
}
