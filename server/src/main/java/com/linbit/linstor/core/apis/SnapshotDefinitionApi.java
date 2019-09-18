package com.linbit.linstor.core.apis;

import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition.SnapshotVlmDfnApi;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface SnapshotDefinitionApi
{
    ResourceDefinitionApi getRscDfn();
    UUID getUuid();
    String getSnapshotName();
    long getFlags();
    Map<String, String> getProps();
    List<SnapshotVolumeDefinition.SnapshotVlmDfnApi> getSnapshotVlmDfnList();
}