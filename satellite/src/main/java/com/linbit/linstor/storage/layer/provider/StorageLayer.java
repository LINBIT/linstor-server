package com.linbit.linstor.storage.layer.provider;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.Resource;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.Snapshot;
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
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.MdSuperblockBuffer;
import com.linbit.linstor.storage.layer.provider.lvm.LvmProvider;
import com.linbit.linstor.storage.layer.provider.lvm.LvmThinProvider;
import com.linbit.linstor.storage.layer.provider.swordfish.SwordfishInitiatorProvider;
import com.linbit.linstor.storage.layer.provider.swordfish.SwordfishTargetProvider;
import com.linbit.linstor.storage.layer.provider.utils.Commands;
import com.linbit.linstor.storage.layer.provider.zfs.ZfsProvider;
import com.linbit.linstor.storage.layer.provider.zfs.ZfsThinProvider;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.utils.AccessUtils;
import com.linbit.utils.Pair;

import javax.inject.Provider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class StorageLayer implements DeviceLayer
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


    public StorageLayer(
        ExtCmdFactory extCmdFactoryRef,
        AccessContext storDriverAccCtxRef,
        StltConfigAccessor stltConfigAccessorRef,
        ErrorReporter errorReporterRef,
        Provider<TransactionMgr> transMgrProviderRef,
        DeviceLayer.NotificationListener notificationListenerRef
    )
    {
        extCmdFactory = extCmdFactoryRef;
        storDriverAccCtx = storDriverAccCtxRef;
        errorReporter = errorReporterRef;
        notificationListener = notificationListenerRef;

        lvmDriver = new LvmProvider(
            extCmdFactoryRef,
            storDriverAccCtxRef,
            stltConfigAccessorRef,
            transMgrProviderRef,
            notificationListenerRef,
            this,
            errorReporterRef
        );
        lvmThinDriver = new LvmThinProvider(
            extCmdFactoryRef,
            storDriverAccCtxRef,
            stltConfigAccessorRef,
            transMgrProviderRef,
            notificationListenerRef,
            this,
            errorReporterRef
        );
        zfsDriver = new ZfsProvider(
            extCmdFactoryRef,
            storDriverAccCtxRef,
            notificationListenerRef
        );
        zfsThinDriver = new ZfsThinProvider(
            extCmdFactoryRef,
            storDriverAccCtxRef,
            notificationListenerRef
        );
        sfTargetDriver = new SwordfishTargetProvider(
            notificationListenerRef
        );
        sfInitDriver = new SwordfishInitiatorProvider(
            notificationListenerRef
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
    public Map<Resource, StorageException> adjustTopDown(Collection<Resource> resources, Collection<Snapshot> snapshots)
        throws StorageException
    {
        // no-op, we only have to perform one adjust on this layer
        return Collections.emptyMap();
    }

    @Override
    public Map<Resource, StorageException> adjustBottomUp(
        Collection<Resource> resources,
        Collection<Snapshot> snapshots
    )
    {
        Map<Volume, StorageException> exceptions = new HashMap<>();
        Map<DeviceProvider, List<Volume>> groupedVolumes = resources.parallelStream()
            .flatMap(Resource::streamVolumes)
            .collect(Collectors.groupingBy(this::classifier));

        for (Entry<DeviceProvider, List<Volume>> entry : groupedVolumes.entrySet())
        {
            try
            {
                List<Volume> volumes = entry.getValue();
                if (!volumes.isEmpty())
                {
                    exceptions.putAll(
                        entry.getKey().adjust(
                            volumes
                        )
                    );
                }
            }
            catch (StorageException exc)
            {
                errorReporter.reportError(exc);
            }
        }

        List<Resource> successResources = new ArrayList<>(resources);
        for (Entry<Volume, StorageException> entry : exceptions.entrySet())
        {
            // Volume vlm = entry.getKey();
            StorageException storExc = entry.getValue();
            // TODO set FAILED state to volume?
            // or report back to devMgr -> Controller?

            errorReporter.reportError(storExc);
            successResources.remove(entry.getKey().getResource());
        }

        Map<Boolean, List<Resource>> processedResources = successResources.stream()
            .collect(Collectors.partitioningBy(this::isDeleteFlagSet));
        processedResources.get(false).forEach(notificationListener::notifyResourceApplied);
        processedResources.get(true).forEach(notificationListener::notifyResourceDeleted);

        return convertExceptions(exceptions);
    }

    private Map<Resource, StorageException> convertExceptions(Map<Volume, StorageException> exceptions)
    {
        Map<Resource, List<Pair<Volume, StorageException>>> groupedExceptions = new HashMap<>();
        for (Entry<Volume, StorageException> entry : exceptions.entrySet())
        {
            Volume vlm = entry.getKey();
            groupedExceptions.computeIfAbsent(
                vlm.getResource(),
                ignore -> new ArrayList<>()
            )
                .add(new Pair<>(vlm, entry.getValue()));
        }
        Map<Resource, StorageException> returnedExceptions = new HashMap<>();
        for (Entry<Resource, List<Pair<Volume, StorageException>>> entry : groupedExceptions.entrySet())
        {
            Resource rsc = entry.getKey();
            List<Pair<Volume, StorageException>> list = entry.getValue();

            List<Integer> vlmNrs = new ArrayList<>();
            StringBuilder details = new StringBuilder();
            for (Pair<Volume, StorageException> pair : list)
            {
                Volume vlm = pair.objA;
                int vlmNr = vlm.getVolumeDefinition().getVolumeNumber().value;
                vlmNrs.add(vlmNr);
                details.append("Volume ").append(vlmNr).append(" details: \n");
                StorageException vlmExc = pair.objB;
                details.append("Message:       ").append(vlmExc.getMessage());
                appendIfValueNotNull(details, "\nCause:       ", vlmExc.getCauseText());
                appendIfValueNotNull(details, "\nDescription: ", vlmExc.getDescriptionText());
                appendIfValueNotNull(details, "\nCorrection:  ", vlmExc.getCorrectionText());
                appendIfValueNotNull(details, "\nDetails:     ", vlmExc.getDetailsText());
                details.append("\n");
            }
            StringBuilder descr = new StringBuilder("Volumes ")
                .append(vlmNrs.toString())
                .append(" failed");

            returnedExceptions.put(
                rsc,
                new StorageException(
                    "Resource failed: " + rsc,
                    descr.toString(),
                    null,
                    null,
                    details.toString()
                )
            );
        }
        return returnedExceptions;
    }

    private void appendIfValueNotNull(StringBuilder builder, String key, String value)
    {
        if (value != null)
        {
            builder.append(key).append(value);
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

    private boolean isDeleteFlagSet(Resource rsc)
    {
        return AccessUtils.execPrivileged(
            () -> rsc.getStateFlags().isSet(storDriverAccCtx, RscFlags.DELETE),
            "Storage Layer has not enough privileges"
        );
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
    public void quickWipe(String devicePath) throws StorageException, IOException
    {
        Commands.wipeFs(extCmdFactory.create(), devicePath);
        MdSuperblockBuffer.wipe(devicePath);
    }

    public void wipe(Collection<String> devicePaths, Consumer<String> wipeFinishedNotifier)
    {
        // TODO: this step should be asynchron
        /*
         * for security reasons we should wipe (zero out) an lvm / zfs before actually removing it.
         *
         * however, user may want to skip this step for performance reasons.
         * in that case, we still need to make sure to at least wipe DRBD's signature so that
         * re-allocating the same storage does not find the data-garbage from last DRBD configuration
         */

        for (String devicePath : devicePaths)
        {
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
}
