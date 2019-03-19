package com.linbit.linstor.storage.interfaces.layers.luks;

import com.linbit.linstor.storage.interfaces.categories.VlmLayerObject;

public interface LuksVlmObject extends VlmLayerObject
{
    byte[] getEncryptedKey();
}
