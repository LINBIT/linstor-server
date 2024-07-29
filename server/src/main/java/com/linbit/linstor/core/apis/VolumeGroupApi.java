package com.linbit.linstor.core.apis;

import com.linbit.linstor.annotation.Nullable;

import java.util.Map;
import java.util.UUID;

public interface VolumeGroupApi
{
    @Nullable
    Integer getVolumeNr();
    Map<String, String> getProps();
    UUID getUUID();
    long getFlags();
}
