package com.linbit.linstor.api.interfaces;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import com.fasterxml.jackson.annotation.JsonIgnore;

public interface RscDfnLayerDataApi
{
    String getRscNameSuffix();

    @JsonIgnore
    DeviceLayerKind getLayerKind();
}
