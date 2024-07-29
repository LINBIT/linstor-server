package com.linbit.linstor.core.apis;

import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;

import com.linbit.linstor.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface ResourceGroupApi
{
    @Nullable UUID getUuid();
    String getName();
    String getDescription();
    Map<String, String> getProps();
    @Nullable AutoSelectFilterApi getAutoSelectFilter();
    List<VolumeGroupApi> getVlmGrpList();
    @Nullable Short getPeerSlots();
}
