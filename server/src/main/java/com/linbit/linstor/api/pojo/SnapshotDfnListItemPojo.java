package com.linbit.linstor.api.pojo;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotDefinition.SnapshotDfnApi;
import com.linbit.linstor.SnapshotVolumeDefinition;

import java.util.List;
import java.util.UUID;

public class SnapshotDfnListItemPojo implements SnapshotDefinition.SnapshotDfnListItemApi
{
    private final SnapshotDfnApi snapshotDfnApi;
    private final List<String> nodeNames;

    public SnapshotDfnListItemPojo(
        SnapshotDfnApi snapshotDfnPojoRef,
        List<String> nodeNamesRef
    )
    {
        snapshotDfnApi = snapshotDfnPojoRef;
        nodeNames = nodeNamesRef;
    }

    @Override
    public ResourceDefinition.RscDfnApi getRscDfn()
    {
        return snapshotDfnApi.getRscDfn();
    }

    @Override
    public UUID getUuid()
    {
        return snapshotDfnApi.getUuid();
    }

    @Override
    public String getSnapshotName()
    {
        return snapshotDfnApi.getSnapshotName();
    }

    @Override
    public long getFlags()
    {
        return snapshotDfnApi.getFlags();
    }

    @Override
    public List<SnapshotVolumeDefinition.SnapshotVlmDfnApi> getSnapshotVlmDfnList()
    {
        return snapshotDfnApi.getSnapshotVlmDfnList();
    }

    @Override
    public List<String> getNodeNames()
    {
        return nodeNames;
    }
}
