package com.linbit.linstor.storage.interfaces.layers.drbd;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;

public interface DrbdVlmObject<RSC extends AbsResource<RSC>>
    extends VlmLayerObject<RSC>
{
    String getMetaDiskPath();

    String getDiskState();
}
