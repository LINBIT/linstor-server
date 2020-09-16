package com.linbit.linstor.storage.interfaces.layers.nvme;

import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;

public interface OpenflexRscDfnObject extends RscDfnLayerObject
{
    String getNqn();

    String getShortName();
}
