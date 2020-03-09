package com.linbit.linstor.storage.interfaces.layers.nvme;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;

public interface NvmeVlmObject<RSC extends AbsResource<RSC>>
    extends VlmLayerObject<RSC>
{
    String getDiskState();
}
