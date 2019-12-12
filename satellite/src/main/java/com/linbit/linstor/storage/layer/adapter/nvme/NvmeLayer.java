package com.linbit.linstor.storage.layer.adapter.nvme;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;

import static com.linbit.linstor.storage.layer.adapter.nvme.NvmeUtils.NVME_SUBSYSTEMS_PATH;
import static com.linbit.linstor.storage.layer.adapter.nvme.NvmeUtils.NVME_SUBSYSTEM_PREFIX;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Class for managing NVMe Target and Initiator
 *
 * @author Rainer Laschober
 * @since v0.9.6
 */
@Singleton
public class NvmeLayer implements DeviceLayer
{
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
    public LayerProcessResult process(
        AbsRscLayerObject<Resource> rscData,
        List<Snapshot> snapshotList,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        LayerProcessResult ret;
        NvmeRscData<Resource> nvmeRscData = (NvmeRscData<Resource>) rscData;

        if (nvmeRscData.isInitiator(sysCtx))
        {
            // reading a NVMe Target resource associated with a NVMe Initiator to determine if they belong to SPDK
            final Resource targetRsc = nvmeUtils.getTargetResource(nvmeRscData, sysCtx);
            nvmeRscData.setSpdk(nvmeUtils.isSpdkResource(targetRsc.getLayerData(sysCtx)));

            nvmeUtils.setDevicePaths(nvmeRscData, nvmeRscData.exists());

            // disconnect
            if (nvmeRscData.exists() && nvmeRscData.getAbsResource().getStateFlags().isSet(sysCtx, Resource.Flags.DELETE))
            {
                // disconnect
                nvmeUtils.disconnect(nvmeRscData);
            }
            // connect
            else if (!nvmeRscData.exists() &&
                !nvmeRscData.getAbsResource().getStateFlags().isSet(sysCtx, Resource.Flags.DELETE)
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
                errorReporter.logDebug(
                    "NVMe Intiator resource '%s' already in expected state, nothing to be done.",
                    nvmeRscData.getSuffixedResourceName()
                );
            }
            ret = LayerProcessResult.SUCCESS;
        }
        else
        {
            // Target

            // SPDK is only used if all involved volumes belong to SPDK
            nvmeRscData.setSpdk(nvmeUtils.isSpdkResource(nvmeRscData));

            nvmeRscData.setExists(nvmeUtils.isTargetConfigured(nvmeRscData));

            if (nvmeRscData.getAbsResource().getStateFlags().isSet(sysCtx, Resource.Flags.DELETE))
            {
                if (nvmeRscData.exists())
                {
                    // delete target resource
                    nvmeUtils.deleteTargetRsc(nvmeRscData, sysCtx);
                    resourceProcessorProvider.get().process(nvmeRscData.getSingleChild(), snapshotList, apiCallRc);
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
                    final String subsystemName = nvmeUtils.getNvmeSubsystemPrefix(nvmeRscData)
                            + nvmeRscData.getSuffixedResourceName();
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
                            if (((Volume) nvmeVlmData.getVolume()).getFlags().isSet(sysCtx, Volume.Flags.DELETE))
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

                        LayerProcessResult layerProcessResult = resourceProcessorProvider.get()
                            .process(nvmeRscData.getSingleChild(), snapshotList, apiCallRc);

                        if (layerProcessResult == LayerProcessResult.SUCCESS)
                        {
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
                    LayerProcessResult layerProcessResult = resourceProcessorProvider.get()
                        .process(nvmeRscData.getSingleChild(), snapshotList, apiCallRc);
                    if (layerProcessResult == LayerProcessResult.SUCCESS)
                    {
                        nvmeUtils.createTargetRsc(nvmeRscData, sysCtx);
                    }
                }
            }
            ret = LayerProcessResult.NO_DEVICES_PROVIDED;
        }
        return ret;
    }

    @Override
    public void updateAllocatedSizeFromUsableSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException
    {
        NvmeVlmData<Resource> nvmeVlmData = (NvmeVlmData<Resource>) vlmData;

        // basically no-op. gross == net for NVMe
        long size = nvmeVlmData.getUsableSize();
        nvmeVlmData.setAllocatedSize(size);

        VlmProviderObject<Resource> childVlmData = nvmeVlmData.getSingleChild();
        childVlmData.setUsableSize(size);
        resourceProcessorProvider.get().updateAllocatedSizeFromUsableSize(childVlmData);
    }

    @Override
    public void updateUsableSizeFromAllocatedSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException
    {
        NvmeVlmData<Resource> nvmeVlmData = (NvmeVlmData<Resource>) vlmData;

        // basically no-op. gross == net for NVMe
        long size = nvmeVlmData.getAllocatedSize();
        nvmeVlmData.setUsableSize(size);

        VlmProviderObject<Resource> childVlmData = nvmeVlmData.getSingleChild();
        childVlmData.setAllocatedSize(size);
        resourceProcessorProvider.get().updateUsableSizeFromAllocatedSize(childVlmData);
    }

    @Override
    public void clearCache()
        throws StorageException
    {
        // no-op
    }

    @Override
    public void setLocalNodeProps(Props localNodeProps)
    {
        // no-op
    }

    @Override
    public void resourceFinished(AbsRscLayerObject<Resource> layerDataRef)
        throws AccessDeniedException
    {
        NvmeRscData<Resource> nvmeRscData = (NvmeRscData<Resource>) layerDataRef;
        if (nvmeRscData.getAbsResource().getStateFlags().isSet(sysCtx, Resource.Flags.DELETE))
        {
            resourceProcessorProvider.get().sendResourceDeletedEvent(nvmeRscData);
        }
        else
        {
            resourceProcessorProvider.get().sendResourceCreatedEvent(
                nvmeRscData,
                new UsageState(
                    true,
                    null, // will be mapped to unknown
                    true
                )
            );
        }
    }
}
