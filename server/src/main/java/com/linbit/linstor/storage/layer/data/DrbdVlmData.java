package com.linbit.linstor.storage.layer.data;

import com.linbit.linstor.storage.layer.data.categories.VlmLayerData;

public interface DrbdVlmData extends VlmLayerData
{
    String getMetaDiskPath();

    String getDiskState();
}
