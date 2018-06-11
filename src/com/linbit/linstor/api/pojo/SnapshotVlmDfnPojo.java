package com.linbit.linstor.api.pojo;

import com.linbit.linstor.SnapshotVolumeDefinition;

import java.util.UUID;

public class SnapshotVlmDfnPojo implements SnapshotVolumeDefinition.SnapshotVlmDfnApi
{
    private final UUID uuid;
    private final Integer volumeNr;
    private final long size;

    public SnapshotVlmDfnPojo(UUID uuidRef, Integer volumeNrRef, long sizeRef)
    {
        uuid = uuidRef;
        volumeNr = volumeNrRef;
        size = sizeRef;
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

    @Override
    public long getSize()
    {
        return size;
    }
}
