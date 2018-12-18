package com.linbit.linstor.core.devmgr.helper;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.LvmThinDriverKind;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageDriverKind;
import com.linbit.linstor.storage.ZfsDriverKind;
import com.linbit.linstor.storage.ZfsThinDriverKind;
import com.linbit.linstor.storage.layer.provider.lvm.LvmProvider;
import com.linbit.linstor.storage.layer.provider.lvm.LvmThinLayerDataStlt;
import com.linbit.linstor.storage.layer.provider.zfs.ZfsLayerDataStlt;
import com.linbit.linstor.storage.layer.provider.zfs.ZfsProvider;
import com.linbit.linstor.storage2.layer.data.categories.VlmLayerData;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class LayeredSnapshotHelper
{
    private interface LayerDataUpdater
    {
        void update(SnapshotVolume snapVlm) throws AccessDeniedException, SQLException, InvalidKeyException;
    }

    private final AccessContext sysCtx;

    @Inject
    public LayeredSnapshotHelper(
        @SystemContext AccessContext sysCtxRef
    )
    {
        sysCtx = sysCtxRef;
    }

    public void updateSnapshotLayerData(Collection<Resource> dfltResources, Collection<Snapshot> snapshots)
    {
        try
        {
            Map<ResourceName, Resource> resourcesByName = new TreeMap<>();
            for (Resource rsc : dfltResources)
            {
                resourcesByName.put(rsc.getDefinition().getName(), rsc);
            }

            // currently only SnapshotVolumes have layerData, not Snapshot.
            for (Snapshot snapshot : snapshots)
            {
                for (SnapshotVolume snapVlm : snapshot.getAllSnapshotVolumes(sysCtx))
                {
                    StorPool storPool = snapVlm.getStorPool(sysCtx);
                    StorageDriverKind storDriverKind = storPool.getDriverKind();

                    LayerDataUpdater updater = null;
                    if (storDriverKind instanceof LvmThinDriverKind)
                    {
                        updater = this::lvmThinUpdater;
                    }
                    if (storDriverKind instanceof ZfsDriverKind ||
                        storDriverKind instanceof ZfsThinDriverKind)
                    {
                        updater = this::zfsUpdater;
                    }
                    if (updater == null)
                    {
                        throw new ImplementationError("Snapshot from " + storDriverKind + " should not exist");
                    }

                    updater.update(snapVlm);
                }
            }
        }
        catch (AccessDeniedException | SQLException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void lvmThinUpdater(SnapshotVolume snapVlm) throws AccessDeniedException, SQLException, InvalidKeyException
    {
        VlmLayerData layerData = snapVlm.getLayerData(sysCtx);
        if (layerData == null)
        {
            Props storPoolProps = snapVlm.getStorPool(sysCtx)
                .getProps(sysCtx);

            snapVlm.setLayerData(
                sysCtx,
                new LvmThinLayerDataStlt(
                    storPoolProps.getProp(
                        StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY,
                        StorageConstants.NAMESPACE_STOR_DRIVER
                    ),
                    storPoolProps.getProp(
                        StorageConstants.CONFIG_LVM_THIN_POOL_KEY,
                        StorageConstants.NAMESPACE_STOR_DRIVER
                    ),
                    String.format(
                        LvmProvider.FORMAT_RSC_TO_LVM_ID,
                        snapVlm.getSnapshot().getResourceName().displayValue,
                        snapVlm.getVolumeNumber().value
                    )
                )
            );
        }
    }

    private void zfsUpdater(SnapshotVolume snapVlm) throws AccessDeniedException, SQLException, InvalidKeyException
    {
        VlmLayerData layerData = snapVlm.getLayerData(sysCtx);
        if (layerData == null)
        {
            Props storPoolProps = snapVlm.getStorPool(sysCtx)
                .getProps(sysCtx);
            VolumeDefinition vlmDfn = snapVlm.getResourceDefinition().getVolumeDfn(sysCtx, snapVlm.getVolumeNumber());

            snapVlm.setLayerData(
                sysCtx,
                new ZfsLayerDataStlt(
                    storPoolProps.getProp(
                        StorageConstants.CONFIG_ZFS_THIN_POOL_KEY,
                        StorageConstants.NAMESPACE_STOR_DRIVER
                    ),
                    String.format(
                        ZfsProvider.FORMAT_RSC_TO_ZFS_ID,
                        vlmDfn.getResourceDefinition().getName().displayValue,
                        vlmDfn.getVolumeNumber().value
                    )
                )
            );
        }
    }
}
