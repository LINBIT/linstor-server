package com.linbit.linstor.core.apis;

import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition.VlmDfnApi;
import com.linbit.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface ResourceDefinitionApi
{
    UUID getUuid();
    ResourceGroupApi getResourceGroup();
    String getResourceName();
    byte[] getExternalName();
    long getFlags();
    Map<String, String> getProps();
    List<VolumeDefinition.VlmDfnApi> getVlmDfnList();
    List<Pair<String, RscDfnLayerDataApi>> getLayerData();
}