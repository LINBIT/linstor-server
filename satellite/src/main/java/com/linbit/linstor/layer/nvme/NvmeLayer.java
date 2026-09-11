package com.linbit.linstor.layer.nvme;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.linbit.linstor.layer.nvme.NvmeUtils.NVME_SUBSYSTEMS_PATH;
import static com.linbit.linstor.layer.nvme.NvmeUtils.NVME_SUBSYSTEM_PREFIX;

/**
 * Class for managing NVMe Target and Initiator
 *
 * @author Rainer Laschober
 *
 * @since v0.9.6
 */
@Singleton
public class NvmeLayer implements DeviceLayer
{
    private static final String SUSPEND_IO_NOT_SUPPORTED_ERR_MSG =
        "Suspending / Resuming IO for NVMe resources is not supported";
    private final ErrorReporter errorReporter;
    private final Provider<DeviceHandler> resourceProcessorProvider;
    private final NvmeUtils nvmeUtils;

    @Inject
    public NvmeLayer(
        ErrorReporter errorReporterRef,
        NvmeUtils nvmeUtilsRef,
        Provider<DeviceHandler> resourceProcessorRef
    )
    {
        errorReporter = errorReporterRef;
        nvmeUtils = nvmeUtilsRef;
        resourceProcessorProvider = resourceProcessorRef;
    }

    @Override
    public String getName()
    {
        return this.getClass().getSimpleName();
    }

    @Override
    public void prepare(
        Set<AbsRscLayerObject<Resource>> rscDataList,
        Set<AbsRscLayerObject<Snapshot>> affectedSnapshots
    )
        throws StorageException, DatabaseException
    {
        // no-op
    }

    @Override
    public boolean isSuspendIoSupported()
    {
        return false;
    }

    @Override
    public void suspendIo(AbsRscLayerObject<Resource> ignoredRscDataRef, boolean ignoredAsRootLayerRef)
        throws ExtCmdFailedException, StorageException
    {
        throw new StorageException(SUSPEND_IO_NOT_SUPPORTED_ERR_MSG);
    }

    @Override
    public void resumeIo(AbsRscLayerObject<Resource> ignoredRscDataRef, boolean ignoredAsRootLayerRef)
        throws ExtCmdFailedException, StorageException
    {
        throw new StorageException(SUSPEND_IO_NOT_SUPPORTED_ERR_MSG);
    }

    @Override
    public void updateSuspendState(AbsRscLayerObject<Resource> rscDataRef)
        throws DatabaseException, ExtCmdFailedException, StorageException
    {
        throw new StorageException(SUSPEND_IO_NOT_SUPPORTED_ERR_MSG);
    }

    /**
     * Connects/disconnects an NVMe Target or creates/deletes its data.
     *
     * @param rscData
     *     RscLayerObject object to processed.
     *     If diskless, rscData is an NVMe Initiator and a Target otherwise.
     *     Depending on its {@link Flags} the operation executed on the Initiator/Target is either
     *     connect/configure or disconnect/delete.
     * @param apiCallRc
     *     ApiCallRcImpl responses, passed on to {@link DeviceHandler}
     */
    @Override
    public void processResource(
        AbsRscLayerObject<Resource> rscData,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, DatabaseException
    {
        NvmeRscData<Resource> nvmeRscData = (NvmeRscData<Resource>) rscData;

        StateFlags<Flags> rscFlags = nvmeRscData.getAbsResource().getStateFlags();
        boolean isRscDeleting = rscFlags.isSomeSet(
            Resource.Flags.DELETE,
            Resource.Flags.DISK_REMOVING,
            Resource.Flags.INACTIVE
        );
        if (nvmeRscData.isInitiator())
        {
            // reading a NVMe Target resource associated with a NVMe Initiator to determine if they belong to SPDK
            final Resource targetRsc = nvmeUtils.getTargetResource(nvmeRscData);
            nvmeRscData.setSpdk(nvmeUtils.isSpdkResource(targetRsc.getLayerData()));


            nvmeUtils.setDevicePaths(nvmeRscData, nvmeRscData.exists() && !isRscDeleting);

            // disconnect
            if (nvmeRscData.exists() && isRscDeleting)
            {
                // disconnect
                nvmeUtils.disconnect(nvmeRscData);
            }
            // connect
            else if (!nvmeRscData.exists() && !isRscDeleting)
            {
                // connect
                nvmeUtils.connect(nvmeRscData);
                if (!nvmeUtils.setDevicePaths(nvmeRscData, true))
                {
                    throw new StorageException("Failed to set NVMe device path!");
                }
            }
            else
            {
                boolean cleanedUpVlm = false;
                for (NvmeVlmData<Resource> nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
                {
                    // if volumes-/definitions get deleted, nvme will take care of removing the device accordingly
                    // however, we still need to set those vlmData to not exists so that the deviceHandler does not
                    // complain about us not having properly cleaned up
                    if (NvmeUtils.isVlmDeleted(nvmeVlmData))
                    {
                        nvmeVlmData.setExists(false);
                        errorReporter.logTrace(
                            "NVMe volume '%d' of resource '%s' deleted",
                            nvmeVlmData.getVlmNr().value,
                            nvmeVlmData.getRscLayerObject().getSuffixedResourceName()
                        );
                        cleanedUpVlm = true;
                    }
                }
                if (!cleanedUpVlm)
                {
                    errorReporter.logDebug(
                        "NVMe Intiator resource '%s' already in expected state, nothing to be done.",
                        nvmeRscData.getSuffixedResourceName()
                    );
                }
            }
        }
        else
        {
            // Target

            // SPDK is only used if all involved volumes belong to SPDK
            nvmeRscData.setSpdk(nvmeUtils.isSpdkResource(nvmeRscData));

            nvmeRscData.setExists(nvmeUtils.isTargetConfigured(nvmeRscData));

            if (isRscDeleting)
            {
                if (nvmeRscData.exists())
                {
                    // delete target resource
                    nvmeUtils.deleteTargetRsc(nvmeRscData);
                    resourceProcessorProvider.get().processResource(nvmeRscData.getSingleChild(), apiCallRc);
                }
                else
                {
                    errorReporter.logDebug(
                        "NVMe target resource '%s' already in expected state, nothing to be done.",
                        nvmeRscData.getSuffixedResourceName()
                    );
                }
            }
            else
            {
                if (nvmeRscData.exists())
                {
                    // Update volumes
                    final String subsystemName = NvmeUtils.getNvmeSubsystemPrefix(nvmeRscData) +
                        nvmeRscData.getSuffixedResourceName();
                    final String subsystemDirectory = NVME_SUBSYSTEMS_PATH + subsystemName;

                    try
                    {
                        errorReporter.logDebug(
                            "NVMe: updating target volumes: " +
                                NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName()
                        );

                        List<NvmeVlmData<Resource>> newVolumes = new ArrayList<>();
                        for (NvmeVlmData<Resource> nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
                        {
                            if (((Volume) nvmeVlmData.getVolume()).getFlags()
                                .isSomeSet(
                                    Volume.Flags.DELETE,
                                    Volume.Flags.CLONING
                                ))
                            {
                                if (nvmeRscData.isSpdk())
                                {
                                    nvmeUtils.deleteSpdkNamespace(nvmeVlmData, subsystemName);
                                }
                                else
                                {
                                    nvmeUtils.deleteNamespace(nvmeVlmData, subsystemDirectory);
                                }
                            }
                            else
                            {
                                newVolumes.add(nvmeVlmData);
                            }
                        }

                        resourceProcessorProvider.get().processResource(nvmeRscData.getSingleChild(), apiCallRc);

                        for (NvmeVlmData<Resource> nvmeVlmData : newVolumes)
                        {
                            if (nvmeRscData.isSpdk())
                            {
                                nvmeUtils.createSpdkNamespace(nvmeVlmData, subsystemName);
                            }
                            else
                            {
                                nvmeUtils.createNamespace(nvmeVlmData, subsystemDirectory);
                            }
                        }
                    }
                    catch (IOException | ChildProcessTimeoutException exc)
                    {
                        throw new StorageException("Failed to update NVMe target!", exc);
                    }
                }
                else
                {
                    // Create volumes
                    resourceProcessorProvider.get().processResource(nvmeRscData.getSingleChild(), apiCallRc);
                    nvmeUtils.createTargetRsc(nvmeRscData);
                }
            }
        }
    }

    @Override
    public void clearCache()
        throws StorageException
    {
        // no-op
    }

    @Override
    public @Nullable LocalPropsChangePojo setLocalNodeProps(Props localNodeProps)
    {
        // no-op
        return null;
    }

    @Override
    public boolean resourceFinished(AbsRscLayerObject<Resource> layerDataRef)
    {
        NvmeRscData<Resource> nvmeRscData = (NvmeRscData<Resource>) layerDataRef;
        if (nvmeRscData.getAbsResource()
            .getStateFlags()
            .isSomeSet(
                Resource.Flags.DELETE,
                Resource.Flags.DISK_REMOVING
            ))
        {
            resourceProcessorProvider.get().sendResourceDeletedEvent(nvmeRscData);
        }
        else
        {
            resourceProcessorProvider.get().sendResourceCreatedEvent(
                nvmeRscData,
                new ResourceState(
                    true,
                    // no (drbd) connections to peers
                    Collections.emptyMap(),
                    null, // will be mapped to unknown
                    true,
                    null,
                    null
                )
            );
        }
        return true;
    }

    @Override
    public boolean isDeleteFlagSet(AbsRscLayerObject<?> rscDataRef)
    {
        return false; // no layer specific DELETE flag
    }

    @Override
    public CloneSupportResult getCloneSupport(
        AbsRscLayerObject<?> ignoredSourceRef,
        AbsRscLayerObject<?> ignoredTargetRef
    )
    {
        return CloneSupportResult.PASSTHROUGH;
    }

    @Override
    public DeviceLayerKind getKind()
    {
        return DeviceLayerKind.NVME;
    }
}
