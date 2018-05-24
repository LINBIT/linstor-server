package com.linbit.linstor.api.pojo;

import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;

import java.util.UUID;

public class SnapshotDfnPojo implements SnapshotDefinition.SnapshotDfnApi
{
    private final UUID uuid;
    private final String snapshotName;
    private final UUID rscDfnUuid;
    private final String rscName;
    private final long flags;

    public SnapshotDfnPojo(
        UUID uuidRef,
        String snapshotNameRef,
        UUID rscDfnUuidRef,
        String rscNameRef,
        long flagsRef
    )
    {
        uuid = uuidRef;
        snapshotName = snapshotNameRef;
        rscDfnUuid = rscDfnUuidRef;
        rscName = rscNameRef;
        flags = flagsRef;
    }

    @Override
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public String getSnapshotName()
    {
        return snapshotName;
    }

    @Override
    public UUID getRscDfnUuid()
    {
        return rscDfnUuid;
    }

    @Override
    public String getRscName()
    {
        return rscName;
    }

    @Override
    public long getFlags()
    {
        return flags;
    }
}
