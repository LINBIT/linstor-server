package com.linbit.linstor.storage.interfaces.layers.luks;

import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;

public interface LuksVlmObject extends VlmLayerObject
{
    byte[] getEncryptedKey();
}
