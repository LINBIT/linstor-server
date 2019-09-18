package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.SnapshotDefinitionApi;
import com.linbit.linstor.core.apis.SnapshotVolumeDefinitionApi;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SnapshotDfnPojo implements SnapshotDefinitionApi
{
    private final ResourceDefinitionApi rscDfn;
    private final UUID uuid;
    private final String snapshotName;
    private final List<SnapshotVolumeDefinitionApi> snapshotVlmDfns;
    private final long flags;
    private final Map<String, String> props;

    public SnapshotDfnPojo(
        ResourceDefinitionApi rscDfnRef,
        UUID uuidRef,
        String snapshotNameRef,
        List<SnapshotVolumeDefinitionApi> snapshotVlmDfnsRef,
        long flagsRef,
        Map<String, String> propsRef
    )
    {
        rscDfn = rscDfnRef;
        uuid = uuidRef;
        snapshotName = snapshotNameRef;
        snapshotVlmDfns = snapshotVlmDfnsRef;
        flags = flagsRef;
        props = propsRef;
    }

    @Override
    public ResourceDefinitionApi getRscDfn()
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
    public List<SnapshotVolumeDefinitionApi> getSnapshotVlmDfnList()
    {
        return snapshotVlmDfns;
    }

    @Override
    public long getFlags()
    {
        return flags;
    }

    @Override
    public Map<String, String> getProps()
    {
        return props;
    }
}
