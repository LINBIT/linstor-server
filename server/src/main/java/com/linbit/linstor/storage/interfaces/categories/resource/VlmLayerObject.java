package com.linbit.linstor.storage.interfaces.categories.resource;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

public interface VlmLayerObject<RSC extends AbsResource<RSC>>
    extends VlmProviderObject<RSC>
{
    default int getRscLayerId()
    {
        return getRscLayerObject().getRscLayerId();
    }

    default String getDataDevice()
    {
        return getSingleChild().getDevicePath();
    }

    default VlmProviderObject<RSC> getSingleChild()
    {
        return getRscLayerObject().getSingleChild().getVlmProviderObject(getVlmNr());
    }

    @Override
    default DeviceProviderKind getProviderKind()
    {
        return DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER;
    }

    default @Nullable VlmProviderObject<RSC> getChildBySuffix(String suffixRef)
    {
        AbsRscLayerObject<RSC> childBySuffix = getRscLayerObject().getChildBySuffix(suffixRef);
        VlmProviderObject<RSC> ret = null;
        if (childBySuffix != null)
        {
            ret = childBySuffix.getVlmProviderObject(getVlmNr());
        }
        return ret;
    }

    /*
     * Layers above StorageLayer usually do not have special storage pools, but either pass the processing
     * to the lower layer (eventually to the StorageLayer) or do not pass the processing at all.
     */
    @Override
    default @Nullable StorPool getStorPool()
    {
        return null;
    }

    @Override
    default void setStorPool(AccessContext accCtx, StorPool storPoolRef) throws DatabaseException, AccessDeniedException
    {
        // no-op, see getStorPool()
    }
}
