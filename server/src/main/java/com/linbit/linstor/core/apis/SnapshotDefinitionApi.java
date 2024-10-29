package com.linbit.linstor.core.apis;

import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface SnapshotDefinitionApi
{
    ResourceDefinitionApi getRscDfn();
    UUID getUuid();
    String getSnapshotName();
    long getFlags();
    Map<String, String> getSnapDfnProps();
    Map<String, String> getRscDfnProps();
    List<SnapshotVolumeDefinitionApi> getSnapshotVlmDfnList();
    List<Pair<String, RscDfnLayerDataApi>> getLayerData();
    List<SnapshotApi> getSnapshots();
}
