package com.linbit.linstor.storage.interfaces.categories;

import com.linbit.linstor.storage.kinds.DeviceProviderKind;

public interface VlmLayerObject extends VlmProviderObject
{
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
}
