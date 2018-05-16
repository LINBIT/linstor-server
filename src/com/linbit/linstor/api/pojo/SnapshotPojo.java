package com.linbit.linstor.api.pojo;

import com.linbit.linstor.Snapshot;

import java.util.UUID;

public class SnapshotPojo implements Snapshot.SnapshotApi
{
    private final UUID uuid;
    private final String name;
    private final UUID snapshotDfnUuid;
    private final boolean suspendResource;
    private final boolean takeSnapshot;

    public SnapshotPojo(
        UUID uuidRef,
        String nameRef,
        UUID snapshotDfnUuidRef,
        boolean suspendResourceRef,
        boolean takeSnapshotRef
    )
    {
        uuid = uuidRef;
        name = nameRef;
        snapshotDfnUuid = snapshotDfnUuidRef;
        suspendResource = suspendResourceRef;
        takeSnapshot = takeSnapshotRef;
    }

    @Override
    public UUID getSnapshotUuid()
    {
        return uuid;
    }

    @Override
    public String getSnapshotName()
    {
        return name;
    }

    @Override
    public UUID getSnapshotDfnUuid()
    {
        return snapshotDfnUuid;
    }

    @Override
    public boolean getSuspendResource()
    {
        return suspendResource;
    }

    @Override
    public boolean getTakeSnapshot()
    {
        return takeSnapshot;
    }
}
