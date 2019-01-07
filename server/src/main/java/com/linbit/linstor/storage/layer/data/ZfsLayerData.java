package com.linbit.linstor.storage.layer.data;

import com.linbit.linstor.storage.layer.data.categories.VlmLayerData;

public interface ZfsLayerData extends VlmLayerData
{
    String getZPool();

    String getIdentifier();
}
