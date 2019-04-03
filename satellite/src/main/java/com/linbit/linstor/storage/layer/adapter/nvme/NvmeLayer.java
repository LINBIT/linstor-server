package com.linbit.linstor.storage.layer.adapter.nvme;

import com.linbit.linstor.NetInterface;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;

import static com.linbit.linstor.storage.layer.adapter.nvme.NvmeUtils.NVME_SUBSYSTEM_PREFIX;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

@Singleton
public class NvmeLayer implements DeviceLayer
{
    private final AccessContext sysCtx;
    private final Provider<DeviceHandler> resourceProcessorProvider;
    private final NvmeUtils nvmeUtils;

    @Inject
    public NvmeLayer(
        @DeviceManagerContext AccessContext workerCtxRef,
        NvmeUtils nvmeUtilsRef,
        Provider<DeviceHandler> resourceProcessorRef
    )
    {
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
        throws StorageException, AccessDeniedException, SQLException
    {
        // no-op
    }

    @Override
    public void process(RscLayerObject rscData, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, SQLException
    {
        NvmeRscData nvmeRscData = (NvmeRscData) rscData;
        NetInterface netIf = nvmeRscData.getResource().getAssignedNode().getSatelliteConnection(sysCtx);

        // initiator
        if (nvmeRscData.isDiskless(sysCtx))
        {
            if (nvmeRscData.exists() && nvmeRscData.getResource().getStateFlags().isSet(sysCtx, RscFlags.DELETE))
            {
                int vlmCount = nvmeRscData.getResource().getVolumeCount();
                for (int vlmNr = 0; vlmNr < vlmCount; vlmNr++)
                {
                    nvmeUtils.disconnect(
                        NVME_SUBSYSTEM_PREFIX + nvmeRscData.getResourceName() + "_" + vlmNr,
                        10 // FIXME: dummy
                    );
                }

                nvmeRscData.setExists(false);
            }
            else if (!nvmeRscData.exists() && !nvmeRscData.getResource().getStateFlags().isSet(sysCtx, RscFlags.DELETE))
            {
                nvmeUtils.connect(netIf.getAddress(sysCtx).getAddress());
                nvmeRscData.setExists(true);
            }
        }
        // target
        else
        {
            if (nvmeRscData.exists() && nvmeRscData.getResource().getStateFlags().isSet(sysCtx, RscFlags.DELETE))
            {
                nvmeUtils.cleanUpTarget(nvmeRscData, sysCtx);
                resourceProcessorProvider.get().process(
                    nvmeRscData.getSingleChild(),
                    snapshots,
                    apiCallRc
                );

                nvmeRscData.setExists(false);
            }
            else if (!nvmeRscData.exists() && !nvmeRscData.getResource().getStateFlags().isSet(sysCtx, RscFlags.DELETE))
            {
                resourceProcessorProvider.get().process(
                    nvmeRscData.getSingleChild(),
                    snapshots,
                    apiCallRc
                );

                nvmeUtils.configureTarget(nvmeRscData, sysCtx);
                nvmeRscData.setExists(true);
            }
        }
    }

    @Override
    public void updateGrossSize(VlmProviderObject vlmData)
        throws AccessDeniedException, SQLException
    {
        NvmeVlmData nvmeVlmData = (NvmeVlmData) vlmData;

        long allocatedSize = nvmeVlmData.getParentAllocatedSizeOrElse(
            () -> nvmeVlmData.getVolume().getVolumeDefinition().getVolumeSize(sysCtx)
        );

        nvmeVlmData.setUsableSize(allocatedSize);
        nvmeVlmData.setAllocatedSize(allocatedSize);
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
        if (nvmeRscData.getResource().getStateFlags().isSet(sysCtx, RscFlags.DELETE))
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
