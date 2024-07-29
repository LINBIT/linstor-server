package com.linbit.linstor.core.apis;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface VolumeDefinitionApi
{
    @Nullable
    UUID getUuid();

    @Nullable
    Integer getVolumeNr();
    long getSize();
    long getFlags();
    Map<String, String> getProps();
    List<Pair<String, VlmDfnLayerDataApi>> getVlmDfnLayerData();
}
