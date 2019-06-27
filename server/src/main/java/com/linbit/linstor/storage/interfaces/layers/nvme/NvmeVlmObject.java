package com.linbit.linstor.storage.interfaces.layers.nvme;

import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

public interface NvmeVlmObject extends VlmLayerObject
{
    String getDiskState();

    @Override
    default DeviceProviderKind getProviderKind()
    {
        return DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER;
    }
}
