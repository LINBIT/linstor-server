package com.linbit.linstor.storage.layer.data;

import com.linbit.linstor.storage.layer.data.categories.VlmLayerData;

public interface LvmLayerData extends VlmLayerData
{
    String getVolumeGroup();

    String getIdentifier();
}
