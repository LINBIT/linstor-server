package com.linbit.linstor.layer.nvme;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import static com.linbit.linstor.layer.nvme.NvmeUtils.NVME_SUBSYSTEMS_PATH;
import static com.linbit.linstor.layer.nvme.NvmeUtils.NVME_SUBSYSTEM_PREFIX;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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
    private final AccessContext sysCtx;
    private final Provider<DeviceHandler> resourceProcessorProvider;
    private final NvmeUtils nvmeUtils;

    @Inject
    public NvmeLayer(
        ErrorReporter errorReporterRef,
        @DeviceManagerContext AccessContext workerCtxRef,
        NvmeUtils nvmeUtilsRef,
        Provider<DeviceHandler> resourceProcessorRef
    )
    {
        errorReporter = errorReporterRef;
        sysCtx = workerCtxRef;
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
        throws StorageException, AccessDeniedException, DatabaseException
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
     * @param snapshotList
     *     Collection<Snapshot> to be processed, passed on to {@link DeviceHandler}
     * @param apiCallRc
     *     ApiCallRcImpl responses, passed on to {@link DeviceHandler}
     */
    @Override
    public void processResource(
        AbsRscLayerObject<Resource> rscData,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        NvmeRscData<Resource> nvmeRscData = (NvmeRscData<Resource>) rscData;

        StateFlags<Flags> rscFlags = nvmeRscData.getAbsResource().getStateFlags();
        if (nvmeRscData.isInitiator(sysCtx))
        {
            // reading a NVMe Target resource associated with a NVMe Initiator to determine if they belong to SPDK
            final Resource targetRsc = nvmeUtils.getTargetResource(nvmeRscData, sysCtx);
            nvmeRscData.setSpdk(nvmeUtils.isSpdkResource(targetRsc.getLayerData(sysCtx)));

            nvmeUtils.setDevicePaths(nvmeRscData, nvmeRscData.exists());

            // disconnect
            if (
                nvmeRscData.exists() && rscFlags.isSomeSet(sysCtx, Resource.Flags.DELETE,
                    Resource.Flags.DISK_REMOVING,
                    Resource.Flags.INACTIVE
                )
            )
            {
                // disconnect
                nvmeUtils.disconnect(nvmeRscData);
            }
            // connect
            else if (!nvmeRscData.exists() &&
                !rscFlags.isSomeSet(
                    sysCtx,
                    Resource.Flags.DELETE,
                    Resource.Flags.DISK_REMOVING,
                    Resource.Flags.INACTIVE
                )
            )
            {
                // connect
                nvmeUtils.connect(nvmeRscData, sysCtx);
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
                    AbsVolume<Resource> vlm = nvmeVlmData.getVolume();
                    VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
                    if (((Volume) vlm).getFlags()
                        .isSomeSet(
                            sysCtx,
                            Volume.Flags.DELETE,
                            Volume.Flags.CLONING
                        ) || vlmDfn.getFlags().isSet(sysCtx, VolumeDefinition.Flags.DELETE))
                    {
                        nvmeVlmData.setExists(false);
                        errorReporter.logTrace(
                            "NVMe volume '%d' of resource '%s' deleted",
                            vlmDfn.getVolumeNumber().value,
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

            if (rscFlags.isSomeSet(
                sysCtx,
                Resource.Flags.DELETE,
                Resource.Flags.DISK_REMOVING,
                Resource.Flags.INACTIVE
            ))
            {
                if (nvmeRscData.exists())
                {
                    // delete target resource
                    nvmeUtils.deleteTargetRsc(nvmeRscData, sysCtx);
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
                                    sysCtx,
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
                    catch (AccessDeniedException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                }
                else
                {
                    // Create volumes
                    resourceProcessorProvider.get().processResource(nvmeRscData.getSingleChild(), apiCallRc);
                    nvmeUtils.createTargetRsc(nvmeRscData, sysCtx);
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
        throws AccessDeniedException
    {
        NvmeRscData<Resource> nvmeRscData = (NvmeRscData<Resource>) layerDataRef;
        if (nvmeRscData.getAbsResource()
            .getStateFlags()
            .isSomeSet(
                sysCtx,
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
