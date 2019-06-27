package com.linbit.linstor.storage.interfaces.categories.resource;

import com.linbit.linstor.StorPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.sql.SQLException;

public interface VlmLayerObject extends VlmProviderObject
{
    default int getRscLayerId()
    {
        return getRscLayerObject().getRscLayerId();
    }

    default String getBackingDevice()
    {
        return getSingleChild().getDevicePath();
    }

    default VlmProviderObject getSingleChild()
    {
        return getRscLayerObject().getSingleChild().getVlmProviderObject(getVlmNr());
    }

    @Override
    default DeviceProviderKind getProviderKind()
    {
        return DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER;
    }

    default VlmProviderObject getChildBySuffix(String suffixRef)
    {
        return getRscLayerObject().getChildBySuffix(suffixRef).getVlmProviderObject(getVlmNr());
    }

    /*
     * Layers above StorageLayer usually do not have special storage pools, but either pass the processing
     * to the lower layer (eventually to the StorageLayer) or do not pass the processing at all.
     */
    @Override
    default StorPool getStorPool()
    {
        return null;
    }

    @Override
    default void setStorPool(AccessContext accCtx, StorPool storPoolRef) throws SQLException, AccessDeniedException
    {
        // no-op, see getStorPool()
    }
}
