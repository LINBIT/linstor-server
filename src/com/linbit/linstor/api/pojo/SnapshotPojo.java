package com.linbit.linstor.api.pojo;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Snapshot;

import java.util.UUID;

public class SnapshotPojo implements Snapshot.SnapshotApi
{
    private final ResourceDefinition.RscDfnApi rscDfn;
    private final UUID uuid;
    private final String name;
    private final UUID snapshotDfnUuid;
    private final boolean suspendResource;
    private final boolean takeSnapshot;
    private final Long fullSyncId;
    private final Long updateId;

    public SnapshotPojo(
        ResourceDefinition.RscDfnApi rscDfnRef,
        UUID uuidRef,
        String nameRef,
        UUID snapshotDfnUuidRef,
        boolean suspendResourceRef,
        boolean takeSnapshotRef,
        Long fullSyncIdRef,
        Long updateIdRef
    )
    {
        rscDfn = rscDfnRef;
        uuid = uuidRef;
        name = nameRef;
        snapshotDfnUuid = snapshotDfnUuidRef;
        suspendResource = suspendResourceRef;
        takeSnapshot = takeSnapshotRef;
        fullSyncId = fullSyncIdRef;
        updateId = updateIdRef;
    }

    @Override
    public ResourceDefinition.RscDfnApi getRscDfn()
    {
        return rscDfn;
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
