package com.linbit.linstor.storage.layer.provider;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.Snapshot.SnapshotFlags;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class StorageLayer implements ResourceLayer
{
    private final AccessContext storDriverAccCtx;

    private final LvmProvider lvmDriver;
    private final LvmThinProvider lvmThinDriver;
    private final ZfsProvider zfsDriver;
    private final ZfsThinProvider zfsThinDriver;
    private final SwordfishTargetProvider sfTargetDriver;
    private final SwordfishInitiatorProvider sfInitDriver;
    private final NotificationListener notificationListener;
    private final List<DeviceProvider> driverList;

    public StorageLayer(
        ExtCmdFactory extCmdFactoryRef,
        AccessContext storDriverAccCtxRef,
        StltConfigAccessor stltConfigAccessorRef,
        ErrorReporter errorReporterRef,
        NotificationListener notificationListenerRef
    )
    {
        storDriverAccCtx = storDriverAccCtxRef;
        notificationListener = notificationListenerRef;

        WipeHandler wipeHandler = new WipeHandler(extCmdFactoryRef, errorReporterRef);

        lvmDriver = new LvmProvider(
            errorReporterRef,
            extCmdFactoryRef,
            storDriverAccCtxRef,
            stltConfigAccessorRef,
            wipeHandler,
            notificationListenerRef
        );
        lvmThinDriver = new LvmThinProvider(
            errorReporterRef,
            extCmdFactoryRef,
            storDriverAccCtxRef,
            stltConfigAccessorRef,
            wipeHandler,
            notificationListenerRef
        );
        zfsDriver = new ZfsProvider(
            errorReporterRef,
            extCmdFactoryRef,
            storDriverAccCtxRef,
            stltConfigAccessorRef,
            wipeHandler,
            notificationListenerRef
        );
        zfsThinDriver = new ZfsThinProvider(
            errorReporterRef,
            extCmdFactoryRef,
            storDriverAccCtxRef,
            stltConfigAccessorRef,
            wipeHandler,
            notificationListenerRef
        );
        sfTargetDriver = new SwordfishTargetProvider(
            notificationListenerRef
        );
        sfInitDriver = new SwordfishInitiatorProvider(
            notificationListenerRef
        );

        driverList = Arrays.asList(
            lvmDriver,
            lvmThinDriver,
            zfsDriver,
            zfsThinDriver,
            sfTargetDriver,
            sfInitDriver
        );
    }

    @Override
    public void setLocalNodeProps(Props localNodeProps)
    {
        lvmDriver.setLocalNodeProps(localNodeProps);
        lvmThinDriver.setLocalNodeProps(localNodeProps);
        zfsDriver.setLocalNodeProps(localNodeProps);
        zfsThinDriver.setLocalNodeProps(localNodeProps);
        sfTargetDriver.setLocalNodeProps(localNodeProps);
        sfInitDriver.setLocalNodeProps(localNodeProps);
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
            notificationListener.notifyResourceDeleted(rsc);
            // rsc.delete is done by the deviceManager
        }
        else
        {
            notificationListener.notifyResourceApplied(rsc);
        }

        for (Snapshot snapshot : snapshots)
        {
            if (snapshot.getFlags().isSet(storDriverAccCtx, SnapshotFlags.DELETE))
            {
                notificationListener.notifySnapshotDeleted(snapshot);
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
            devProvider = lvmDriver;
        }
        else if (driverKind instanceof LvmThinDriverKind)
        {
            devProvider = lvmThinDriver;
        }
        else if (driverKind instanceof ZfsDriverKind)
        {
            devProvider = zfsDriver;
        }
        else if (driverKind instanceof ZfsThinDriverKind)
        {
            devProvider = zfsThinDriver;
        }
        else if (driverKind instanceof SwordfishTargetDriverKind)
        {
            devProvider = sfTargetDriver;
        }
        else if (driverKind instanceof SwordfishInitiatorDriverKind)
        {
            devProvider = sfInitDriver;
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
