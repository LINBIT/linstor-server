package com.linbit.linstor.core.apis;

import java.util.UUID;

public interface SnapshotVolumeApi
{
    UUID getSnapshotVlmUuid();
    UUID getSnapshotVlmDfnUuid();
    String getStorPoolName();
    UUID getStorPoolUuid();
    int getSnapshotVlmNr();
}