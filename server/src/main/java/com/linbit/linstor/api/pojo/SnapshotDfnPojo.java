package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.SnapshotApi;
import com.linbit.linstor.core.apis.SnapshotDefinitionApi;
import com.linbit.linstor.core.apis.SnapshotVolumeDefinitionApi;
import com.linbit.utils.Pair;

import java.util.Collections;
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
    private final Map<String, String> snapDfnProps;
    private final Map<String, String> rscDfnProps;
    private final List<Pair<String, RscDfnLayerDataApi>> layerData;
    private final List<SnapshotApi> snapshots;

    public SnapshotDfnPojo(
        ResourceDefinitionApi rscDfnRef,
        UUID uuidRef,
        String snapshotNameRef,
        List<SnapshotVolumeDefinitionApi> snapshotVlmDfnsRef,
        long flagsRef,
        Map<String, String> snapDfnPropsMapRef,
        Map<String, String> rscDfnPropsMapRef,
        List<Pair<String, RscDfnLayerDataApi>> layerDataRef,
        List<SnapshotApi> snapshotsRef
    )
    {
        rscDfn = rscDfnRef;
        uuid = uuidRef;
        snapshotName = snapshotNameRef;
        snapshotVlmDfns = snapshotVlmDfnsRef;
        flags = flagsRef;
        snapDfnProps = Collections.unmodifiableMap(snapDfnPropsMapRef);
        rscDfnProps = Collections.unmodifiableMap(rscDfnPropsMapRef);
        layerData = layerDataRef;
        snapshots = snapshotsRef;
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
    public Map<String, String> getSnapDfnProps()
    {
        return snapDfnProps;
    }

    @Override
    public Map<String, String> getRscDfnProps()
    {
        return rscDfnProps;
    }

    @Override
    public List<Pair<String, RscDfnLayerDataApi>> getLayerData()
    {
        return layerData;
    }

    @Override
    public List<SnapshotApi> getSnapshots()
    {
        return snapshots;
    }
}
