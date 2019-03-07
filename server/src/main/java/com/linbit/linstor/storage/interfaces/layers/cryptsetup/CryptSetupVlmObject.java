package com.linbit.linstor.storage.interfaces.layers.cryptsetup;

import com.linbit.linstor.storage.interfaces.categories.VlmLayerObject;

public interface CryptSetupVlmObject extends VlmLayerObject
{
    byte[] getEncryptedKey();
}
