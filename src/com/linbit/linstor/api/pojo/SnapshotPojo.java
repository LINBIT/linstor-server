package com.linbit.linstor.api.pojo;

import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;

import java.util.UUID;

public class SnapshotPojo implements Snapshot.SnapshotApi
{
    private final SnapshotDefinition.SnapshotDfnApi snaphotDfn;
    private final UUID uuid;
    private final boolean suspendResource;
    private final boolean takeSnapshot;
    private final Long fullSyncId;
    private final Long updateId;

    public SnapshotPojo(
        SnapshotDefinition.SnapshotDfnApi snaphotDfnRef,
        UUID uuidRef,
        boolean suspendResourceRef,
        boolean takeSnapshotRef,
        Long fullSyncIdRef,
        Long updateIdRef
    )
    {
        snaphotDfn = snaphotDfnRef;
        uuid = uuidRef;
        suspendResource = suspendResourceRef;
        takeSnapshot = takeSnapshotRef;
        fullSyncId = fullSyncIdRef;
        updateId = updateIdRef;
    }

    @Override
    public SnapshotDefinition.SnapshotDfnApi getSnaphotDfn()
    {
        return snaphotDfn;
    }

    @Override
    public UUID getSnapshotUuid()
    {
        return uuid;
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

    @Override
    public Long getFullSyncId()
    {
        return fullSyncId;
    }

    @Override
    public Long getUpdateId()
    {
        return updateId;
    }
}
