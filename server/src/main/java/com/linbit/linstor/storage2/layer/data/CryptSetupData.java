package com.linbit.linstor.storage2.layer.data;

import com.linbit.linstor.storage2.layer.data.categories.RscLayerData;

public interface CryptSetupData extends RscLayerData
{
    char[] getPassword();
}
