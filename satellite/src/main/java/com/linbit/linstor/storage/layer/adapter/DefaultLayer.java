package com.linbit.linstor.storage.layer.adapter;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.devmgr.DeviceHandler2;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.ResourceLayer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
import com.linbit.linstor.storage.utils.ResourceUtils;
import com.linbit.utils.RemoveAfterDevMgrRework;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@RemoveAfterDevMgrRework
@Singleton
public class DefaultLayer implements ResourceLayer
{
    private final AccessContext sysCtx;
    private final Provider<NotificationListener> notificationListener;
    private final Provider<DeviceHandler2> resourceProcessor;

    private Props localNodeProps;

    @Inject
    public DefaultLayer(
        @DeviceManagerContext AccessContext sysCtxRef,
        Provider<NotificationListener> notificationListenerProviderRef,
        Provider<DeviceHandler2> resourceProcessorProviderRef
    )
    {
        sysCtx = sysCtxRef;
        notificationListener = notificationListenerProviderRef;
        resourceProcessor = resourceProcessorProviderRef;
    }

    @Override
    public String getName()
    {
        return this.getClass().getSimpleName();
    }

    @Override
    public void prepare(List<Resource> value, List<Snapshot> snapshots)
        throws StorageException, AccessDeniedException, SQLException
    {
        // no-op
    }

    @Override
    public void updateGrossSize(Volume dfltVlm, Volume parentVolume) throws AccessDeniedException, SQLException
    {
        if (parentVolume != null)
        {
            throw new ImplementationError(
                "Default volume should not have a parent. \ndefault volume: " + dfltVlm +
                    "\nparent volume: " + parentVolume
            );
        }
        long size = dfltVlm.getVolumeDefinition().getVolumeSize(sysCtx);

        dfltVlm.setAllocatedSize(sysCtx, size);
        dfltVlm.setUsableSize(sysCtx, size);
    }

    @Override
    public void clearCache() throws StorageException
    {
        // no-op
    }

    @Override
    public void process(Resource rsc, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, SQLException
    {
        Resource child = ResourceUtils.getSingleChild(rsc, sysCtx);
        resourceProcessor.get().process(child, snapshots, apiCallRc);

        // delete the resource / volume if all went well (that means, no exception from the previous .process call)

        if (rsc.getStateFlags().isSet(sysCtx, RscFlags.DELETE))
        {
            apiCallRc.addEntry(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.DELETED | ApiConsts.MASK_RSC,
                    "Resource '" + rsc.getDefinition().getName() + "' [DEFAULT] deleted"
                )
            );

            notificationListener.get().notifyResourceDeleted(rsc);
        }
        else
        {
            for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
            {
                if (vlm.getFlags().isSet(sysCtx, VlmFlags.DELETE))
                {
                    /*
                     * TODO: call the following method once the API is split into
                     * notifyVolumeDeleted(volume); // deletion of typed volume / no change in FreeSpace
                     * notifyStorageVolumeDeleted(volume, newFreeSpace);
                     */
                }
                else
                {
                    if (!child.isDeleted()  && child.getStateFlags().isSet(sysCtx, RscFlags.DELETE))
                    {
                        // copy the topmost (typed) volume's device paths
                        Volume childVlm = child.getVolume(vlm.getVolumeDefinition().getVolumeNumber());
                        vlm.setDevicePath(
                            sysCtx,
                            childVlm.getDevicePath(sysCtx)
                        );
                        vlm.setUsableSize(
                            sysCtx,
                            childVlm.getUsableSize(sysCtx)
                        );
                    }
                }
            }

            apiCallRc.addEntry(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.DELETED | ApiConsts.MASK_RSC,
                    "Resource '" + rsc.getDefinition().getName() + "' [DEFAULT] applied"
                )
            );
            notificationListener.get().notifyResourceApplied(rsc);
        }
    }

    @Override
    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }
}
