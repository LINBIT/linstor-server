package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Iterator;
import java.util.List;

/**
 * Class for checking if a resource creation is valid according to type/role-specific constraints
 *
 * @author Rainer Laschober
 * @since v0.9.6
 */
@Singleton
public class ResourceCreateCheck
{

    @Inject
    public ResourceCreateCheck()
    {
    }

    private enum ResourceRole
    {
        NVME_TARGET,
        NVME_INITIATOR
    }

    /**
     * Checks if the resource creation is valid according to its type and role.
     * For example, an NVMe Target has different constraints than a NVMe Initiator
     *
     * @throws ApiRcException if any constraint is violated
     */
    public void checkCreatedResource(Resource rsc)
    {
        // first check RD if it has already other resources with some properties like nvmeTarget, drbd, ...
        ResourceDefinition rscDfn = rsc.getResourceDefinition();

        boolean rdHasNvmeTarget = false;
        boolean rdHasDrbd = !rscDfn.getLayerData(DeviceLayerKind.DRBD).isEmpty();

        Iterator<Resource> rscIt = rscDfn.iterateResource();
        while (rscIt.hasNext())
        {
            Resource otherRsc = rscIt.next();
            if (!otherRsc.equals(rsc))
            {
                List<DeviceLayerKind> layerStack = LayerRscUtils.getLayerStack(otherRsc);
                if (layerStack.contains(DeviceLayerKind.NVME))
                {
                    if (!otherRsc.isNvmeInitiator())
                    {
                        rdHasNvmeTarget = true;
                    }
                }
            }
        }

        // now check the new resource's role and validate it
        ResourceRole resourceRole = getCreatedResourceRole(rsc);
        if (resourceRole != null)
        {
            switch (resourceRole)
            {
                case NVME_TARGET ->
                {
                    if (rdHasNvmeTarget && !rdHasDrbd)
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_EXISTS_NVME_TARGET_PER_RSC_DFN,
                                "Only one NVMe Target per resource definition allowed!"
                            )
                        );
                    }
                }
                case NVME_INITIATOR ->
                {
                    if (!rdHasNvmeTarget)
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_MISSING_NVME_TARGET,
                                "An NVMe Target needs to be created before the Initiator!"
                            )
                        );
                    }
                }
                default ->
                {
                    // no further checks needed in this case
                }
            }
        }
    }

    private @Nullable ResourceRole getCreatedResourceRole(Resource rsc)
    {
        ResourceRole ret = null;

        List<DeviceLayerKind> layerStack = LayerUtils.getLayerStack(rsc);
        if (layerStack.contains(DeviceLayerKind.NVME))
        {
            ret = rsc.isNvmeInitiator() ?
                ResourceRole.NVME_INITIATOR :
                ResourceRole.NVME_TARGET;
        }
        return ret;
    }
}
