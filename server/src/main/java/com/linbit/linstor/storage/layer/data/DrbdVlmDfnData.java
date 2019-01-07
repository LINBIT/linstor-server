package com.linbit.linstor.storage.layer.data;

import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.storage.layer.data.categories.VlmDfnLayerData;

public interface DrbdVlmDfnData extends VlmDfnLayerData
{
    MinorNumber getMinorNr();

    int getPeerSlots();
}
