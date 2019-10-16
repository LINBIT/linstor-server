package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import static com.linbit.linstor.core.apicallhandler.controller.helpers.ApiUtils.execPrivileged;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.SWORDFISH_INITIATOR;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.SWORDFISH_TARGET;

import javax.inject.Inject;
import javax.inject.Singleton;

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
    private AccessContext accessContext;
    private boolean hasNvmeTarget;
    private boolean hasNvmeInitiator;
    private boolean hasSwordfishTarget;
    private boolean hasSwordfishInitiator;

    @Inject
    public ResourceCreateCheck(@ApiContext AccessContext accessContextRef)
    {
        accessContext = accessContextRef;
    }

    private enum ResourceRole
    {
        NVME_TARGET,
        NVME_INITIATOR,
        SWORDFISH_TARGET,
        SWORDFISH_INITIATOR
    }

    /**
     * Checks if the resource creation is valid according to its type and role.
     * For example, an NVMe Target has different constraints than a Swordfish Initiator
     *
     * @throws ApiRcException if any constraint is violated
     */
    public void checkCreatedResource(List<Volume> volumes)
    {
        ResourceRole resourceRole = getCreatedResourceRole(volumes);
        if (resourceRole != null)
        {
            switch (resourceRole)
            {
                case NVME_TARGET:
                    if (hasNvmeTarget)
                    {
                        throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_EXISTS_NVME_TARGET_PER_RSC_DFN,
                            "Only one NVMe Target per resource definition allowed!")
                        );
                    }
                    break;
                case NVME_INITIATOR:
                    if (!hasNvmeTarget)
                    {
                        throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_MISSING_NVME_TARGET,
                            "An NVMe Target needs to be created before the Initiator!")
                        );
                    }
                    break;
                case SWORDFISH_TARGET:
                    if (hasSwordfishTarget)
                    {
                        throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_EXISTS_SWORDFISH_TARGET_PER_RSC_DFN,
                            "Only one Swordfish Target per resource definition allowed!")
                        );
                    }
                    break;
                case SWORDFISH_INITIATOR:
                    if (!hasSwordfishTarget)
                    {
                        throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_MISSING_SWORDFISH_TARGET,
                            "A Swordfish Target needs to be created before the Initiator!")
                        );
                    }
                    break;
                default:
                    // no further checks needed in this case
            }
        }
    }

    /**
     * @param volumes
     *     List<Volume> of the resource being created
     */
    private ResourceRole getCreatedResourceRole(List<Volume> volumes)
    {
        ResourceRole ret = null;

        if (volumes.stream().anyMatch(
                vlm -> execPrivileged(
                    () -> DeviceLayerKind.NVME.equals(
                        vlm.getAbsResource().getLayerData(accessContext).getLayerKind()) &&
                        !vlm.getAbsResource().isNvmeInitiator(accessContext)
                )
        ))
        {
            ret = ResourceRole.NVME_TARGET;
        }
        else
        if (
            volumes.stream().anyMatch(
                vlm -> execPrivileged(
                    () -> DeviceLayerKind.NVME.equals(
                        vlm.getAbsResource().getLayerData(accessContext).getLayerKind()
                    ) && vlm.getAbsResource().isNvmeInitiator(accessContext)
                )
            )
        )
        {
            ret = ResourceRole.NVME_INITIATOR;
        }
        else if (
            volumes.stream()
                .flatMap(vlm -> execPrivileged(() -> LayerVlmUtils.getStorPoolSet(vlm, accessContext).stream()))
                .anyMatch(storpool -> SWORDFISH_TARGET.equals(storpool.getDeviceProviderKind()))
            )
        {
            ret = ResourceRole.SWORDFISH_TARGET;
        }
        else if (
            volumes.stream()
                .flatMap(vlm -> execPrivileged(() -> LayerVlmUtils.getStorPoolSet(vlm, accessContext).stream()))
                .anyMatch(storPool -> DeviceProviderKind.SWORDFISH_INITIATOR.equals(storPool.getDeviceProviderKind()))
            )
        {
            ret = ResourceRole.SWORDFISH_INITIATOR;
        }

        return ret;
    }

    /**
     * Queries a resource definition for existing resources of specific roles (currently NVMe/Swordfish
     * Target/Initiator)
     *
     * @param rscDfn
     *     ResourceDefinition potentially containing a resource of certain roles
     */
    public void getAndSetDeployedResourceRoles(ResourceDefinition rscDfn)
    {
        hasNvmeTarget = execPrivileged(
            () -> rscDfn.streamResource(accessContext)).anyMatch(
                rsc -> execPrivileged(
                    () -> rsc.getLayerData(accessContext).getLayerKind().equals(DeviceLayerKind.NVME) &&
                    !rsc.isNvmeInitiator(accessContext)
                )
        );
        hasNvmeInitiator = execPrivileged(
            () -> rscDfn.streamResource(accessContext)).anyMatch(
                rsc -> execPrivileged(
                    () -> rsc.getLayerData(accessContext).getLayerKind().equals(DeviceLayerKind.NVME) &&
                    rsc.isNvmeInitiator(accessContext)
                )
        );
        hasSwordfishTarget = execPrivileged(
            () -> rscDfn.streamResource(accessContext))
                .flatMap(tmpRsc -> tmpRsc.streamVolumes())
                .flatMap(vlm -> execPrivileged(() -> LayerVlmUtils.getStorPoolSet(vlm, accessContext).stream()))
                .anyMatch(storPool -> SWORDFISH_TARGET.equals(storPool.getDeviceProviderKind())
        );
        hasSwordfishInitiator = execPrivileged(
            () -> rscDfn.streamResource(accessContext))
                .flatMap(tmpRsc -> tmpRsc.streamVolumes())
                .flatMap(vlm -> execPrivileged(() -> LayerVlmUtils.getStorPoolSet(vlm, accessContext).stream()))
                .anyMatch(storPool -> SWORDFISH_INITIATOR.equals(storPool.getDeviceProviderKind())
        );
    }
}
