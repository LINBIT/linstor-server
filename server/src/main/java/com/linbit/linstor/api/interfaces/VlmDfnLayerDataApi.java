package com.linbit.linstor.api.interfaces;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;

public interface VlmDfnLayerDataApi
{
    int getVlmNr();

    DeviceLayerKind getLayerKind();
}
