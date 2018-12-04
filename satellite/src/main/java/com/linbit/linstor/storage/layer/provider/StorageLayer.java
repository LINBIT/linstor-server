package com.linbit.linstor.storage.layer.provider;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.Snapshot.SnapshotFlags;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.LvmDriverKind;
import com.linbit.linstor.storage.LvmThinDriverKind;
import com.linbit.linstor.storage.StorageDriverKind;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.SwordfishInitiatorDriverKind;
import com.linbit.linstor.storage.SwordfishTargetDriverKind;
import com.linbit.linstor.storage.ZfsDriverKind;
import com.linbit.linstor.storage.ZfsThinDriverKind;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.ResourceLayer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
import com.linbit.linstor.storage.layer.provider.lvm.LvmProvider;
import com.linbit.linstor.storage.layer.provider.lvm.LvmThinProvider;
import com.linbit.linstor.storage.layer.provider.swordfish.SwordfishInitiatorProvider;
import com.linbit.linstor.storage.layer.provider.swordfish.SwordfishTargetProvider;
import com.linbit.linstor.storage.layer.provider.zfs.ZfsProvider;
import com.linbit.linstor.storage.layer.provider.zfs.ZfsThinProvider;
import com.linbit.utils.AccessUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

@Singleton
public class StorageLayer implements ResourceLayer
{
    private final AccessContext storDriverAccCtx;

    private final LvmProvider lvmProvider;
    private final LvmThinProvider lvmThinProvider;
    private final ZfsProvider zfsProvider;
    private final ZfsThinProvider zfsThinProvider;
    private final SwordfishTargetProvider sfTargetProvider;
    private final SwordfishInitiatorProvider sfInitProvider;
    private final Provider<NotificationListener> notificationListener;
    private final List<DeviceProvider> driverList;

    @Inject
    public StorageLayer(
        @DeviceManagerContext AccessContext storDriverAccCtxRef,
        Provider<NotificationListener> notificationListenerRef,
        LvmProvider lvmProviderRef,
        LvmThinProvider lvmThinProviderRef,
        ZfsProvider zfsProviderRef,
        ZfsThinProvider zfsThinProviderRef,
        SwordfishTargetProvider sfTargetProviderRef,
        SwordfishInitiatorProvider sfInitProviderRef
    )
    {
        storDriverAccCtx = storDriverAccCtxRef;
        notificationListener = notificationListenerRef;

        lvmProvider = lvmProviderRef;
        lvmThinProvider = lvmThinProviderRef;
        zfsProvider = zfsProviderRef;
        zfsThinProvider = zfsThinProviderRef;
        sfTargetProvider = sfTargetProviderRef;
        sfInitProvider = sfInitProviderRef;

        driverList = Arrays.asList(
            lvmProvider,
            lvmThinProvider,
            zfsProvider,
            zfsThinProvider,
            sfTargetProvider,
            sfInitProvider
        );
    }

    @Override
    public void setLocalNodeProps(Props localNodeProps)
    {
        lvmProvider.setLocalNodeProps(localNodeProps);
        lvmThinProvider.setLocalNodeProps(localNodeProps);
        zfsProvider.setLocalNodeProps(localNodeProps);
        zfsThinProvider.setLocalNodeProps(localNodeProps);
        sfTargetProvider.setLocalNodeProps(localNodeProps);
        sfInitProvider.setLocalNodeProps(localNodeProps);
    }

    @Override
    public String getName()
    {
        return this.getClass().getSimpleName();
    }

    @Override
    public void clearCache() throws StorageException
    {
        for (DeviceProvider deviceProvider : driverList)
        {
            deviceProvider.clearCache();
        }
    }

    @Override
    public void prepare(List<Resource> resources, List<Snapshot> snapshots)
        throws StorageException, AccessDeniedException, SQLException
    {
        Map<DeviceProvider, List<Volume>> groupedVolumes = resources.stream()
            .flatMap(Resource::streamVolumes)
            .collect(Collectors.groupingBy(this::classifier));

        Map<DeviceProvider, List<SnapshotVolume>> groupedSnapshotVolumes = snapshots.stream()
            .flatMap(snapshot ->
                AccessUtils.execPrivileged(() -> snapshot.getAllSnapshotVolumes(storDriverAccCtx).stream())
            )
            .collect(Collectors.groupingBy(this::classifier));

        for (Entry<DeviceProvider, List<Volume>> entry : groupedVolumes.entrySet())
        {
            List<Volume> volumes = entry.getValue();
            if (!volumes.isEmpty())
            {
                DeviceProvider deviceProvider = entry.getKey();
                List<SnapshotVolume> snapVlms = groupedSnapshotVolumes.get(deviceProvider);
                if (snapVlms == null)
                {
                    snapVlms = Collections.emptyList();
                }
                deviceProvider.prepare(volumes, snapVlms);
            }
        }
    }

    @Override
    public void process(Resource rsc, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, SQLException
    {
        Map<DeviceProvider, List<Volume>> groupedVolumes = rsc.streamVolumes()
            .collect(Collectors.groupingBy(this::classifier));

        Map<DeviceProvider, List<SnapshotVolume>> groupedSnapshotVolumes = snapshots.stream()
            .flatMap(snapshot ->
                AccessUtils.execPrivileged(() -> snapshot.getAllSnapshotVolumes(storDriverAccCtx).stream())
            )
            .collect(Collectors.groupingBy(this::classifier));

        for (Entry<DeviceProvider, List<Volume>> entry : groupedVolumes.entrySet())
        {
            List<Volume> volumes = entry.getValue();
            DeviceProvider deviceProvider = entry.getKey();
            if (!volumes.isEmpty())
            {
                List<SnapshotVolume> snapVlmList = groupedSnapshotVolumes.get(deviceProvider);
                if (snapVlmList == null)
                {
                    snapVlmList = Collections.emptyList();
                }
                deviceProvider.process(volumes, snapVlmList, apiCallRc);
            }

            for (Volume vlm : volumes)
            {
                if (!vlm.isDeleted() && vlm.getFlags().isSet(storDriverAccCtx, VlmFlags.DELETE))
                {
                    throw new ImplementationError(
                        deviceProvider.getClass().getSimpleName() + " did not delete the volume " + vlm
                    );
                }
            }
        }
        if (rsc.getStateFlags().isSet(storDriverAccCtx, RscFlags.DELETE))
        {
            notificationListener.get().notifyResourceDeleted(rsc);
            // rsc.delete is done by the deviceManager
        }
        else
        {
            notificationListener.get().notifyResourceApplied(rsc);
        }

        for (Snapshot snapshot : snapshots)
        {
            if (snapshot.getFlags().isSet(storDriverAccCtx, SnapshotFlags.DELETE))
            {
                notificationListener.get().notifySnapshotDeleted(snapshot);
                // snapshot.delete is done by the deviceManager
            }
        }
    }

    private DeviceProvider classifier(Volume vlm)
    {
        DeviceProvider devProvider = null;
        try
        {
            devProvider = classifier(vlm.getStorPool(storDriverAccCtx));
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return devProvider;
    }

    private DeviceProvider classifier(SnapshotVolume snapVlm)
    {
        DeviceProvider devProvider = null;
        try
        {
            devProvider = classifier(snapVlm.getStorPool(storDriverAccCtx));
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return devProvider;
    }

    private DeviceProvider classifier(StorPool storPool)
    {
        StorageDriverKind driverKind = storPool.getDriverKind();

        DeviceProvider devProvider;
        if (driverKind instanceof LvmDriverKind)
        {
            devProvider = lvmProvider;
        }
        else if (driverKind instanceof LvmThinDriverKind)
        {
            devProvider = lvmThinProvider;
        }
        else if (driverKind instanceof ZfsDriverKind)
        {
            devProvider = zfsProvider;
        }
        else if (driverKind instanceof ZfsThinDriverKind)
        {
            devProvider = zfsThinProvider;
        }
        else if (driverKind instanceof SwordfishTargetDriverKind)
        {
            devProvider = sfTargetProvider;
        }
        else if (driverKind instanceof SwordfishInitiatorDriverKind)
        {
            devProvider = sfInitProvider;
        }
        else
        {
            throw new ImplementationError("Unknown storagerProvider found: " +
                driverKind.getDriverName() + " " + driverKind.getClass().getSimpleName()
            );
        }
        return devProvider;
    }
}
