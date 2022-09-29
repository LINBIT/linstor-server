package com.linbit.linstor.core.apis;

import java.util.Map;
import java.util.UUID;

public interface SnapshotVolumeApi
{
    UUID getSnapshotVlmUuid();
    UUID getSnapshotVlmDfnUuid();
    int getSnapshotVlmNr();
    Map<String, String> getPropsMap();
    String getState();
}
