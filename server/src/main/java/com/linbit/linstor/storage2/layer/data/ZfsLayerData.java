package com.linbit.linstor.storage2.layer.data;

import com.linbit.linstor.storage2.layer.data.categories.VlmLayerData;

public interface ZfsLayerData extends VlmLayerData
{
    String getZPool();

    String getIdentifier();
}
