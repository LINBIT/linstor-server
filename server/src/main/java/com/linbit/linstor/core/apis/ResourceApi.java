package com.linbit.linstor.core.apis;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.Volume.VlmApi;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface ResourceApi
{
    UUID getUuid();
    String getName();
    UUID getNodeUuid();
    String getNodeName();
    UUID getRscDfnUuid();
    Map<String, String> getProps();
    long getFlags();
    List<? extends Volume.VlmApi> getVlmList();
    RscLayerDataApi getLayerData();
}