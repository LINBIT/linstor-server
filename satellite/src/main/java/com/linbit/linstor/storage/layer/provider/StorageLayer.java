package com.linbit.linstor.storage.layer.provider;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.Snapshot;
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
import com.linbit.linstor.storage.layer.adapter.drbd.utils.MdSuperblockBuffer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
import com.linbit.linstor.storage.layer.provider.lvm.LvmProvider;
import com.linbit.linstor.storage.layer.provider.lvm.LvmThinProvider;
import com.linbit.linstor.storage.layer.provider.swordfish.SwordfishInitiatorProvider;
import com.linbit.linstor.storage.layer.provider.swordfish.SwordfishTargetProvider;
import com.linbit.linstor.storage.layer.provider.utils.Commands;
import com.linbit.linstor.storage.layer.provider.zfs.ZfsProvider;
import com.linbit.linstor.storage.layer.provider.zfs.ZfsThinProvider;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class StorageLayer implements ResourceLayer
{
    private final AccessContext storDriverAccCtx;
    private final ExtCmdFactory extCmdFactory;

    private final LvmProvider lvmDriver;
    private final LvmThinProvider lvmThinDriver;
    private final ZfsProvider zfsDriver;
    private final ZfsThinProvider zfsThinDriver;
    private final SwordfishTargetProvider sfTargetDriver;
    private final SwordfishInitiatorProvider sfInitDriver;
    private final ErrorReporter errorReporter;
    private final NotificationListener notificationListener;
    private final List<DeviceProvider> driverList;

    public StorageLayer(
        ExtCmdFactory extCmdFactoryRef,
        AccessContext storDriverAccCtxRef,
        StltConfigAccessor stltConfigAccessorRef,
        ErrorReporter errorReporterRef,
        Provider<TransactionMgr> transMgrProviderRef,
        NotificationListener notificationListenerRef
    )
    {
        extCmdFactory = extCmdFactoryRef;
        storDriverAccCtx = storDriverAccCtxRef;
        errorReporter = errorReporterRef;
        notificationListener = notificationListenerRef;

        lvmDriver = new LvmProvider(
            errorReporterRef,
            extCmdFactoryRef,
            storDriverAccCtxRef,
            stltConfigAccessorRef,
            this,
            notificationListenerRef
        );
        lvmThinDriver = new LvmThinProvider(
            errorReporterRef,
            extCmdFactoryRef,
            storDriverAccCtxRef,
            stltConfigAccessorRef,
            this,
            notificationListenerRef
        );
        zfsDriver = new ZfsProvider(
            errorReporterRef,
            extCmdFactoryRef,
            storDriverAccCtxRef,
            stltConfigAccessorRef,
            this,
            notificationListenerRef
        );
        zfsThinDriver = new ZfsThinProvider(
            errorReporterRef,
            extCmdFactoryRef,
            storDriverAccCtxRef,
            stltConfigAccessorRef,
            this,
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
    public void prepare(List<Resource> resources) throws StorageException, AccessDeniedException, SQLException
    {
        Map<DeviceProvider, List<Volume>> groupedVolumes = resources.stream()
            .flatMap(Resource::streamVolumes)
            .collect(Collectors.groupingBy(this::classifier));
        for (Entry<DeviceProvider, List<Volume>> entry : groupedVolumes.entrySet())
        {
            List<Volume> volumes = entry.getValue();
            if (!volumes.isEmpty())
            {
                entry.getKey().prepare(volumes);
            }
        }
    }

    @Override
    public void process(Resource rsc, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, SQLException
    {
        Map<DeviceProvider, List<Volume>> groupedVolumes = rsc.streamVolumes()
            .collect(Collectors.groupingBy(this::classifier));

        for (Entry<DeviceProvider, List<Volume>> entry : groupedVolumes.entrySet())
        {
            List<Volume> volumes = entry.getValue();
            DeviceProvider deviceProvider = entry.getKey();
            if (!volumes.isEmpty())
            {
                // TODO: maybe split methods into "process(volumes); handleSnapshots(filteredSnapshots);"
                deviceProvider.process(volumes, null, apiCallRc); // FIXME
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
            rsc.delete(storDriverAccCtx);
        }
        else
        {
            notificationListener.notifyResourceApplied(rsc);
        }

    }

    private DeviceProvider classifier(Volume vlm)
    {
        DeviceProvider devProvider = null;
        try
        {
            StorPool storPool = vlm.getStorPool(storDriverAccCtx);
            StorageDriverKind driverKind = storPool.getDriverKind();

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
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return devProvider;
    }

    /**
     * Only wipes linstor-known data.
     *
     * That means, this method calls "{@code wipefs devicePath}" and cleans drbd super block (last 4k of the device)
     *
     * @param devicePath
     *
     * @throws StorageException
     * @throws IOException
     */
    public void quickWipe(String devicePath) throws StorageException
    {
        Commands.wipeFs(extCmdFactory.create(), devicePath);
        try
        {
            MdSuperblockBuffer.wipe(devicePath);
        }
        catch (IOException ioExc)
        {
            throw new StorageException("Failed to quick-wipe devicePath " + devicePath, ioExc);
        }
    }

    public void asyncWipe(String devicePath, Consumer<String> wipeFinishedNotifier)
    {
        // TODO: this step should be asynchron

        /*
         * for security reasons we should wipe (zero out) an lvm / zfs before actually removing it.
         *
         * however, user may want to skip this step for performance reasons.
         * in that case, we still need to make sure to at least wipe DRBD's signature so that
         * re-allocating the same storage does not find the data-garbage from last DRBD configuration
         */
        try
        {
            MdSuperblockBuffer.wipe(devicePath);
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc);
            // wipe failed, but we still need to free the allocated space
        }
        finally
        {
            wipeFinishedNotifier.accept(devicePath);
        }
    }
}
