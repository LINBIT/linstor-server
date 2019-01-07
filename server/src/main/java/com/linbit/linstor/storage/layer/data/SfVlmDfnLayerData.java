package com.linbit.linstor.storage.layer.data;

import com.linbit.linstor.storage.layer.data.categories.VlmDfnLayerData;

public interface SfVlmDfnLayerData extends VlmDfnLayerData
{
    String getVlmOdata();

    boolean exists();

    boolean isAttached();

    long getAllocatedSize();

    long getUsableSize();
}
