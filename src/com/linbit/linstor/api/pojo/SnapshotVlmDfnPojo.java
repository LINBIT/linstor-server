package com.linbit.linstor.api.pojo;

import com.linbit.linstor.SnapshotVolumeDefinition;

import java.util.UUID;

public class SnapshotVlmDfnPojo implements SnapshotVolumeDefinition.SnapshotVlmDfnApi
{
    private final UUID uuid;
    private final Integer volumeNr;

    public SnapshotVlmDfnPojo(UUID uuidRef, Integer volumeNrRef)
    {
        uuid = uuidRef;
        volumeNr = volumeNrRef;
    }

    @Override
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public Integer getVolumeNr()
    {
        return volumeNr;
    }
}
