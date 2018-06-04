package com.linbit.linstor.api.pojo;

import com.linbit.linstor.SnapshotVolume;

import java.util.Map;
import java.util.UUID;

public class SnapshotVlmPojo implements SnapshotVolume.SnapshotVlmApi
{
    private final String storagePoolName;
    private final UUID storagePoolUuid;
    private final UUID snapshotVlmDfnUuid;
    private final UUID snapshotVlmUuid;
    private final int snapshotVlmNr;

    public SnapshotVlmPojo(
        final String storagePoolNameRef,
        final UUID storagePoolUuidRef,
        final UUID snapshotVlmDfnUuidRef,
        final UUID snapshotVlmUuidRef,
        final int snapshotVlmNrRef
    )
    {
        storagePoolName = storagePoolNameRef;
        storagePoolUuid = storagePoolUuidRef;
        snapshotVlmDfnUuid = snapshotVlmDfnUuidRef;
        snapshotVlmUuid = snapshotVlmUuidRef;
        snapshotVlmNr = snapshotVlmNrRef;
    }

    @Override
    public String getStorPoolName()
    {
        return storagePoolName;
    }

    @Override
    public UUID getStorPoolUuid()
    {
        return storagePoolUuid;
    }

    @Override
    public UUID getSnapshotVlmUuid()
    {
        return snapshotVlmUuid;
    }

    @Override
    public UUID getSnapshotVlmDfnUuid()
    {
        return snapshotVlmDfnUuid;
    }

    @Override
    public int getSnapshotVlmNr()
    {
        return snapshotVlmNr;
    }
}
