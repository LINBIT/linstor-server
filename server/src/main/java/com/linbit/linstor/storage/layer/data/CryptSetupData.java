package com.linbit.linstor.storage.layer.data;

import com.linbit.linstor.storage.layer.data.categories.VlmLayerData;

public interface CryptSetupData extends VlmLayerData
{
    byte[] getPassword();
}
