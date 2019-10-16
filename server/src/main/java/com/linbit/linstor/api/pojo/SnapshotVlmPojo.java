package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.apis.SnapshotVolumeApi;

import java.util.UUID;

public class SnapshotVlmPojo implements SnapshotVolumeApi
{
    private final UUID snapshotVlmDfnUuid;
    private final UUID snapshotVlmUuid;
    private final int snapshotVlmNr;

    public SnapshotVlmPojo(
        final UUID snapshotVlmDfnUuidRef,
        final UUID snapshotVlmUuidRef,
        final int snapshotVlmNrRef
    )
    {
        snapshotVlmDfnUuid = snapshotVlmDfnUuidRef;
        snapshotVlmUuid = snapshotVlmUuidRef;
        snapshotVlmNr = snapshotVlmNrRef;
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
