package com.linbit.linstor.storage2.layer.data;

import com.linbit.linstor.storage2.layer.data.categories.VlmLayerData;

public interface CryptSetupData extends VlmLayerData
{
    byte[] getPassword();
}
