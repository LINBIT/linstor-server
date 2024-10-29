package com.linbit.linstor.core.apis;

import java.util.Map;
import java.util.UUID;

public interface SnapshotVolumeDefinitionApi
{
    UUID getUuid();
    Integer getVolumeNr();
    long getSize();
    long getFlags();
    Map<String, String> getSnapVlmDfnPropsMap();
    Map<String, String> getVlmDfnPropsMap();
}
