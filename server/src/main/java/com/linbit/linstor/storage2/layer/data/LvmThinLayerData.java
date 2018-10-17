package com.linbit.linstor.storage2.layer.data;

import com.linbit.linstor.storage2.layer.data.categories.VlmLayerData;

public interface LvmThinLayerData extends VlmLayerData
{
    String getVolumeGroup();

    String getThinPool();

    String getIdentifier();

    long getActualSize();
}
