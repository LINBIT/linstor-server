package com.linbit.linstor.core.apis;

import java.util.UUID;

public interface SnapshotVolumeDefinitionApi
{
    UUID getUuid();
    Integer getVolumeNr();
    long getSize();
    long getFlags();
}