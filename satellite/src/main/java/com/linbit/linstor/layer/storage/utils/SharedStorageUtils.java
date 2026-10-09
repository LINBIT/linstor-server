package com.linbit.linstor.layer.storage.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;

public class SharedStorageUtils
{
    /**
     * Whether another resource (leg) of the rsc-dfn still needs the shared data of the given volume, i.e. any leg
     * that is not both deleted and inactive. The shared data must not be removed as long as that is the case.
     */
    public static <RSC extends AbsResource<RSC>> boolean isNeededBySharedResource(
        VlmProviderObject<RSC> localVlmData
    )
    {
        return isSharedWithOtherRsc(
            localVlmData,
            otherRsc -> !otherRsc.getStateFlags().isSet(Resource.Flags.DELETE) ||
                !otherRsc.getStateFlags().isSet(Resource.Flags.INACTIVE)
        );
    }

    /**
     * Whether another leg of the rsc-dfn holds a lock on the shared data of the given volume or is about to acquire
     * one: it is active, its deactivation is not confirmed yet, or it is the INACTIVE target of a pending live
     * migration. INACTIVE leftovers of earlier moves do not count.
     */
    public static <RSC extends AbsResource<RSC>> boolean isLockedBySharedResource(
        VlmProviderObject<RSC> localVlmData
    )
    {
        return isSharedWithOtherRsc(localVlmData, SharedStorageUtils::holdsOrAwaitsLock);
    }

    static boolean holdsOrAwaitsLock(Resource otherRsc)
    {
        StateFlags<Flags> otherRscFlags = otherRsc.getStateFlags();
        return !otherRscFlags.isSet(Resource.Flags.INACTIVE) ||
            otherRscFlags.isSet(Resource.Flags.INACTIVATING) ||
            isPendingLiveMigrationTarget(otherRsc);
    }

    private static boolean isPendingLiveMigrationTarget(Resource rsc)
    {
        @Nullable String targetNodeName = rsc.getResourceDefinition().getProps().getProp(
            InternalApiConsts.KEY_LIVE_MIGRATE_SHARED_TARGET_NODE,
            InternalApiConsts.NAMESPC_LIVE_MIGRATE
        );
        return targetNodeName != null && rsc.getNode().getName().displayValue.equalsIgnoreCase(targetNodeName);
    }

    private static <RSC extends AbsResource<RSC>> boolean isSharedWithOtherRsc(
        VlmProviderObject<RSC> localVlmData,
        Predicate<Resource> otherRscFilter
    )
    {
        boolean sharedWithOtherRsc = false;

        AbsRscLayerObject<RSC> localRscStorData = localVlmData.getRscLayerObject();
        SharedStorPoolName localSharedStorPoolName = localVlmData.getStorPool().getSharedStorPoolName();
        RSC localRsc = localVlmData.getVolume().getAbsResource();
        Iterator<Resource> rscIt = localRsc.getResourceDefinition().iterateResource();

        while (rscIt.hasNext() && !sharedWithOtherRsc)
        {
            Resource otherRsc = rscIt.next();
            if (!otherRsc.equals(localRsc) && otherRscFilter.test(otherRsc))
            {
                Set<AbsRscLayerObject<Resource>> otherRscStorDataSet = LayerRscUtils.getRscDataByLayer(
                    otherRsc.getLayerData(),
                    DeviceLayerKind.STORAGE
                );
                for (AbsRscLayerObject<Resource> otherRscStorData : otherRscStorDataSet)
                {
                    if (localRscStorData.getSuffixedResourceName().equals(otherRscStorData.getSuffixedResourceName()))
                    {
                        VlmProviderObject<Resource> otherVlmData = otherRscStorData
                            .getVlmProviderObject(localVlmData.getVlmNr());

                        if (otherVlmData != null)
                        {
                            SharedStorPoolName otherSharedStorPoolName = otherVlmData.getStorPool()
                                .getSharedStorPoolName();

                            if (localSharedStorPoolName.equals(otherSharedStorPoolName))
                            {
                                sharedWithOtherRsc = true;
                                break;
                            }
                        }
                        else if (RscLayerSuffixes.isNonMetaDataLayerSuffix(otherRscStorData.getResourceNameSuffix()))
                        {
                            throw new ImplementationError(
                                "No data volume found for " + otherRscStorData.getAbsResource()
                            );
                        }

                    }
                }
            }
        }

        return sharedWithOtherRsc;
    }
}
