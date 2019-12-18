package com.linbit.linstor.core.apis;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;

import java.util.List;
import java.util.UUID;

public interface SnapshotApi
{
    SnapshotDefinitionApi getSnaphotDfn();
    UUID getSnapshotUuid();
    long getFlags();
    boolean getSuspendResource();
    boolean getTakeSnapshot();
    Long getFullSyncId();
    Long getUpdateId();
    List<? extends SnapshotVolumeApi> getSnapshotVlmList();
    RscLayerDataApi getLayerData();
}