package com.linbit.linstor.api.interfaces;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;

public interface RscDfnLayerDataApi
{
    String getRscNameSuffix();

    DeviceLayerKind getLayerKind();
}
