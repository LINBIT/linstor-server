package com.linbit.linstor.storage.interfaces.layers.drbd;

import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;

public interface DrbdVlmObject extends VlmLayerObject
{
    String getMetaDiskPath();

    String getDiskState();
}
