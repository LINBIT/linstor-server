package com.linbit.linstor.core.apis;

import java.util.Map;
import java.util.UUID;

public interface VolumeGroupApi
{
    Integer getVolumeNr();
    Map<String, String> getProps();
    UUID getUUID();
    long getFlags();
}
