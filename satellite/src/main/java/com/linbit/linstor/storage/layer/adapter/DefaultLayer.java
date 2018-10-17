package com.linbit.linstor.storage.layer.adapter;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.utils.VolumeUtils;
import com.linbit.utils.RemoveAfterDevMgrRework;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@RemoveAfterDevMgrRework
public class DefaultLayer implements DeviceLayer
{
    private final AccessContext sysCtx;
    private final DeviceManager deviceManager;
    private Props localNodeProps;

    public DefaultLayer(
        AccessContext sysCtxRef,
        DeviceManager deviceManagerRef
    )
    {
        sysCtx = sysCtxRef;
        deviceManager = deviceManagerRef;
    }

    @Override
    public Map<Resource, StorageException> adjustTopDown(
        Collection<Resource> resources,
        Collection<Snapshot> snapshots
    )
        throws StorageException
    {
        // no-op
        return Collections.emptyMap();
    }

    @Override
    public Map<Resource, StorageException> adjustBottomUp(
        Collection<Resource> resources,
        Collection<Snapshot> snapshots
    )
        throws StorageException
    {
        Map<Resource, StorageException> exceptions = new HashMap<>();
        try
        {
            for (Resource rsc : resources)
            {
                if (rsc.getStateFlags().isSet(sysCtx, RscFlags.DELETE))
                {
                    if (!rsc.getChildResources(sysCtx).isEmpty())
                    {
                        exceptions.put(
                            rsc,
                            new StorageException(
                                "Resource marked for deletion could not be deleted as it still has children"
                            )
                        );
                    }
                    else
                    {
                        deviceManager.notifyResourceDeleted(rsc);
                    }
                }
                else
                {
                    Iterator<Volume> vlmIt = rsc.iterateVolumes();
                    while (vlmIt.hasNext())
                    {
                        Volume vlm = vlmIt.next();
                        if (vlm.getFlags().isSet(sysCtx, VlmFlags.DELETE))
                        {
                            if (VolumeUtils.getBackingVolume(sysCtx, vlm) != null)
                            {
                                exceptions.put(
                                    rsc,
                                    new StorageException(
                                        "Resource marked for deletion could not be deleted as it still has children"
                                    )
                                );
                                break;
                            }
                            else
                            {
                                deviceManager.notifyVolumeDeleted(vlm, 100_000); // FIXME
                            }
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return exceptions;
    }

    @Override
    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }

}
