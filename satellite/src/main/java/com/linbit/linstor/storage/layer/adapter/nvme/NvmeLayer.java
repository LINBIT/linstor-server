package com.linbit.linstor.storage.layer.adapter.nvme;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.Volume.Flags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.linbit.linstor.storage.layer.adapter.nvme.NvmeUtils.*;

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
    public void prepare(Set<RscLayerObject> rscDataList, Set<Snapshot> affectedSnapshots)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // no-op
    }

    /**
     * Connects/disconnects an NVMe Target or creates/deletes its data.
     *
     * @param rscData   RscLayerObject object to processed.
     *                  If diskless, rscData is an NVMe Initiator and a Target otherwise.
     *                  Depending on its {@link Flags} the operation executed on the Initiator/Target is either
     *                  connect/configure or disconnect/delete.
     * @param snapshots Collection<Snapshot> to be processed, passed on to {@link DeviceHandler}
     * @param apiCallRc ApiCallRcImpl responses, passed on to {@link DeviceHandler}
     */
    @Override
    public void process(RscLayerObject rscData, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        NvmeRscData nvmeRscData = (NvmeRscData) rscData;

        // initiator
        if (nvmeRscData.isDiskless(sysCtx))
        {
            // reading a NVMe Target resource associated with a NVMe Initiator to determine if they belong to SPDK
            final Resource targetRsc = nvmeUtils.getTargetResource(nvmeRscData, sysCtx);
            nvmeRscData.setSpdk(nvmeUtils.isSpdkResource(targetRsc.getLayerData(sysCtx)));

            nvmeUtils.setDevicePaths(nvmeRscData, nvmeRscData.exists());

            // disconnect
            if (nvmeRscData.exists() && nvmeRscData.getResource().getStateFlags().isSet(sysCtx, Resource.Flags.DELETE))
            {
                nvmeUtils.disconnect(nvmeRscData);
            }
            // connect
            else if (!nvmeRscData.exists() &&
                !nvmeRscData.getResource().getStateFlags().isSet(sysCtx, Resource.Flags.DELETE)
            )
            {
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
        }
        // target
        else
        {
            // SPDK is only used if all involved volumes belong to SPDK
            nvmeRscData.setSpdk(nvmeUtils.isSpdkResource(nvmeRscData));

            nvmeRscData.setExists(nvmeUtils.isTargetConfigured(nvmeRscData));

            if (nvmeRscData.getResource().getStateFlags().isSet(sysCtx, Resource.Flags.DELETE))
            {
                // delete target resource
                if (nvmeRscData.exists())
                {
                    nvmeUtils.deleteTargetRsc(nvmeRscData, sysCtx);
                    resourceProcessorProvider.get().process(nvmeRscData.getSingleChild(), snapshots, apiCallRc);
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
                // update volumes
                if (nvmeRscData.exists())
                {
                    final String subsystemName = nvmeUtils.getNvmeSubsystemPrefix(nvmeRscData)
                            + nvmeRscData.getSuffixedResourceName();
                    final String subsystemDirectory = NVME_SUBSYSTEMS_PATH + subsystemName;

                    try
                    {
                        errorReporter.logDebug(
                            "NVMe: updating target volumes: " +
                                NVME_SUBSYSTEM_PREFIX + nvmeRscData.getSuffixedResourceName()
                        );

                        List<NvmeVlmData> newVolumes = new ArrayList<>();
                        for (NvmeVlmData nvmeVlmData : nvmeRscData.getVlmLayerObjects().values())
                        {
                            if (nvmeVlmData.getVolume().getFlags().isSet(sysCtx, Volume.Flags.DELETE))
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

                        resourceProcessorProvider.get().process(nvmeRscData.getSingleChild(), snapshots, apiCallRc);

                        for (NvmeVlmData nvmeVlmData : newVolumes)
                        {
                            if (nvmeRscData.isSpdk())
                            {
                                nvmeUtils.deleteSpdkNamespace(nvmeVlmData, subsystemName);
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
                // create
                else
                {
                    resourceProcessorProvider.get().process(nvmeRscData.getSingleChild(), snapshots, apiCallRc);
                    nvmeUtils.createTargetRsc(nvmeRscData, sysCtx);
                }
            }
        }
    }

    @Override
    public void updateGrossSize(VlmProviderObject vlmData)
        throws AccessDeniedException, DatabaseException
    {
        NvmeVlmData nvmeVlmData = (NvmeVlmData) vlmData;

        long size = nvmeVlmData.getUsableSize();
        nvmeVlmData.setAllocatedSize(size);
        nvmeVlmData.getSingleChild().setUsableSize(size);
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
    public void resourceFinished(RscLayerObject layerDataRef)
        throws AccessDeniedException
    {
        NvmeRscData nvmeRscData = (NvmeRscData) layerDataRef;
        if (nvmeRscData.getResource().getStateFlags().isSet(sysCtx, Resource.Flags.DELETE))
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
