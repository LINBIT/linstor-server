package com.linbit.linstor.core.apis;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface SnapshotApi
{
    SnapshotDefinitionApi getSnaphotDfn();
    String getNodeName();
    UUID getSnapshotUuid();
    long getFlags();
    boolean getSuspendResource();
    boolean getTakeSnapshot();
    Optional<Date> getCreateTimestamp();
    Long getFullSyncId();
    Long getUpdateId();
    List<? extends SnapshotVolumeApi> getSnapshotVlmList();
    RscLayerDataApi getLayerData();
    Map<String, String> getSnapPropsMap();
    Map<String, String> getRscPropsMap();
}
