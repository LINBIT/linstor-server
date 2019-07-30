package com.linbit.linstor.storage.interfaces.layers.drbd;

import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;

public interface DrbdVlmDfnObject extends VlmDfnLayerObject
{
    MinorNumber getMinorNr();
}
