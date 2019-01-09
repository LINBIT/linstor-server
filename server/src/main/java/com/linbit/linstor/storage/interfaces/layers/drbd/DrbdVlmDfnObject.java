package com.linbit.linstor.storage.interfaces.layers.drbd;

import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;

public interface DrbdVlmDfnObject extends VlmDfnLayerObject
{
    MinorNumber getMinorNr();

    int getPeerSlots();
}
