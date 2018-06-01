package com.linbit.linstor.api.pojo;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotVolumeDefinition;

import java.util.List;
import java.util.UUID;

public class SnapshotDfnPojo implements SnapshotDefinition.SnapshotDfnApi
{
    private final ResourceDefinition.RscDfnApi rscDfn;
    private final UUID uuid;
    private final String snapshotName;
    private final List<SnapshotVolumeDefinition.SnapshotVlmDfnApi> snapshotVlmDfns;
    private final long flags;

    public SnapshotDfnPojo(
        ResourceDefinition.RscDfnApi rscDfnRef,
        UUID uuidRef,
        String snapshotNameRef,
        List<SnapshotVolumeDefinition.SnapshotVlmDfnApi> snapshotVlmDfnsRef,
        long flagsRef
    )
    {
        rscDfn = rscDfnRef;
        uuid = uuidRef;
        snapshotName = snapshotNameRef;
        snapshotVlmDfns = snapshotVlmDfnsRef;
        flags = flagsRef;
    }

    @Override
    public ResourceDefinition.RscDfnApi getRscDfn()
    {
        return rscDfn;
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
    public List<SnapshotVolumeDefinition.SnapshotVlmDfnApi> getSnapshotVlmDfnList()
    {
        return snapshotVlmDfns;
    }

    @Override
    public long getFlags()
    {
        return flags;
    }
}
