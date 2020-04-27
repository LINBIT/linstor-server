package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

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
    private final AccessContext apiCtx;

    @Inject
    public ResourceCreateCheck(@ApiContext AccessContext accessContextRef)
    {
        apiCtx = accessContextRef;
    }

    private enum ResourceRole
    {
        NVME_TARGET,
        NVME_INITIATOR,
        OPENFLEX_TARGET,
        OPENFLEX_INITIATOR
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
        try
        {
            ResourceDefinition rscDfn = rsc.getDefinition();

            boolean rdHasNvmeTarget = false;
            boolean rdHasNvmeInitiator = false;
            boolean rdHasDrbd = !rscDfn.getLayerData(apiCtx, DeviceLayerKind.DRBD).isEmpty();
            boolean rdHasOpenflexTarget = false;
            boolean rdHasOpenflexInitiator = false;

            Iterator<Resource> rscIt = rscDfn.iterateResource(apiCtx);
            while (rscIt.hasNext())
            {
                Resource otherRsc = rscIt.next();
                if (!otherRsc.equals(rsc))
                {
                    List<DeviceLayerKind> layerStack = LayerRscUtils.getLayerStack(otherRsc, apiCtx);
                    if (layerStack.contains(DeviceLayerKind.NVME))
                    {
                        if (otherRsc.isNvmeInitiator(apiCtx))
                        {
                            rdHasNvmeInitiator = true;
                        }
                        else
                        {
                            rdHasNvmeTarget = true;
                        }
                    }
                    else
                    {
                        if (layerStack.contains(DeviceLayerKind.OPENFLEX))
                        {
                            if (otherRsc.isNvmeInitiator(apiCtx))
                            {
                                rdHasOpenflexInitiator = true;
                            }
                            else
                            {
                                rdHasOpenflexTarget = true;
                            }
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
                    case NVME_TARGET:
                        if (rdHasNvmeTarget && !rdHasDrbd)
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl.simpleEntry(
                                    ApiConsts.FAIL_EXISTS_NVME_TARGET_PER_RSC_DFN,
                                    "Only one NVMe Target per resource definition allowed!"
                                )
                            );
                        }
                        break;
                    case NVME_INITIATOR:
                        if (!rdHasNvmeTarget)
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl.simpleEntry(
                                    ApiConsts.FAIL_MISSING_NVME_TARGET,
                                    "An NVMe Target needs to be created before the Initiator!"
                                )
                            );
                        }
                        break;
                    case OPENFLEX_TARGET:
                        if (rdHasOpenflexTarget)
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl.simpleEntry(
                                    ApiConsts.FAIL_EXISTS_OPENFLEX_TARGET_PER_RSC_DFN,
                                    "Only one openflex target per resource definition allowed!"
                                )
                            );
                        }
                        break;
                    case OPENFLEX_INITIATOR:
                        if (!rdHasOpenflexTarget)
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl.simpleEntry(
                                    ApiConsts.FAIL_MISSING_NVME_TARGET,
                                    "An Openflex Target needs to be created before the Initiator!"
                                )
                            );
                        }
                    default:
                        // no further checks needed in this case
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private ResourceRole getCreatedResourceRole(Resource rsc)
    {
        ResourceRole ret = null;

        try
        {
            List<DeviceLayerKind> layerStack = LayerUtils.getLayerStack(rsc, apiCtx);
            if (layerStack.contains(DeviceLayerKind.NVME))
            {
                ret = rsc.isNvmeInitiator(apiCtx) ?
                    ResourceRole.NVME_INITIATOR :
                    ResourceRole.NVME_TARGET;
            }
            else
            {
                if (layerStack.contains(DeviceLayerKind.OPENFLEX))
                {
                    ret = rsc.isNvmeInitiator(apiCtx) ?
                        ResourceRole.OPENFLEX_INITIATOR :
                        ResourceRole.OPENFLEX_TARGET;
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }
}
