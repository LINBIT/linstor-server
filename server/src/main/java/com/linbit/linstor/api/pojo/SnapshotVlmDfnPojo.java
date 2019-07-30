package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;

import java.util.UUID;

public class SnapshotVlmDfnPojo implements SnapshotVolumeDefinition.SnapshotVlmDfnApi
{
    private final UUID uuid;
    private final Integer volumeNr;
    private final long size;
    private final long flags;

    public SnapshotVlmDfnPojo(UUID uuidRef, Integer volumeNrRef, long sizeRef, long flagsRef)
    {
        uuid = uuidRef;
        volumeNr = volumeNrRef;
        size = sizeRef;
        flags = flagsRef;
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

    @Override
    public long getFlags()
    {
        return flags;
    }
}
