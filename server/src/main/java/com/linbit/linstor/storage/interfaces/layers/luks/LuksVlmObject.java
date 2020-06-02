package com.linbit.linstor.storage.interfaces.layers.luks;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;

public interface LuksVlmObject<RSC extends AbsResource<RSC>>
    extends VlmLayerObject<RSC>
{
    byte[] getEncryptedKey();
}
