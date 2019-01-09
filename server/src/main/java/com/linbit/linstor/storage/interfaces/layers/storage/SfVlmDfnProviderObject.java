package com.linbit.linstor.storage.interfaces.layers.storage;

import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;

public interface SfVlmDfnProviderObject extends VlmDfnLayerObject
{
    String getVlmOdata();

    boolean exists();

    boolean isAttached();

    long getAllocatedSize();

    long getUsableSize();
}
