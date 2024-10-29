package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.core.apis.SnapshotApi;
import com.linbit.linstor.core.apis.SnapshotDefinitionApi;
import com.linbit.linstor.core.apis.SnapshotVolumeApi;
import com.linbit.linstor.core.objects.AbsResource;

import javax.annotation.Nullable;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class SnapshotPojo implements SnapshotApi, Comparable<SnapshotPojo>
{
    private final SnapshotDefinitionApi snaphotDfn;
    private final UUID uuid;
    private final long flags;
    private final boolean suspendResource;
    private final boolean takeSnapshot;
    private final Long fullSyncId;
    private final Long updateId;
    private final List<SnapshotVolumeApi> snapshotVlms;
    private final RscLayerDataApi layerData;
    private final String nodeName;
    @Nullable
    private final Date createTimestamp;
    private final Map<String, String> snapPropsMap;
    private final Map<String, String> rscPropsMap;

    public SnapshotPojo(
        SnapshotDefinitionApi snaphotDfnRef,
        UUID uuidRef,
        long flagsRef,
        boolean suspendResourceRef,
        boolean takeSnapshotRef,
        Long fullSyncIdRef,
        Long updateIdRef,
        List<SnapshotVolumeApi> snapshotVlmsRef,
        RscLayerDataApi layerDataRef,
        String nodeNameRef,
        @Nullable Date createTimestampRef,
        Map<String, String> snapPropsMapRef,
        Map<String, String> rscPropsMapRef
    )
    {
        snaphotDfn = snaphotDfnRef;
        uuid = uuidRef;
        flags = flagsRef;
        suspendResource = suspendResourceRef;
        takeSnapshot = takeSnapshotRef;
        fullSyncId = fullSyncIdRef;
        updateId = updateIdRef;
        snapshotVlms = snapshotVlmsRef;
        layerData = layerDataRef;
        nodeName = nodeNameRef;
        snapPropsMap = snapPropsMapRef;
        rscPropsMap = rscPropsMapRef;
        createTimestamp = createTimestampRef != null &&
            createTimestampRef.getTime() != AbsResource.CREATE_DATE_INIT_VALUE ?
                createTimestampRef : null;
    }

    @Override
    public SnapshotDefinitionApi getSnaphotDfn()
    {
        return snaphotDfn;
    }

    @Override
    public UUID getSnapshotUuid()
    {
        return uuid;
    }

    @Override
    public long getFlags()
    {
        return flags;
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
    public String getNodeName()
    {
        return nodeName;
    }

    @Override
    public Optional<Date> getCreateTimestamp()
    {
        return Optional.ofNullable(createTimestamp);
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

    @Override
    public List<? extends SnapshotVolumeApi> getSnapshotVlmList()
    {
        return snapshotVlms;
    }

    @Override
    public Map<String, String> getSnapPropsMap()
    {
        return snapPropsMap;
    }

    @Override
    public Map<String, String> getRscPropsMap()
    {
        return rscPropsMap;
    }

    @Override
    public int compareTo(SnapshotPojo otherSnapshotPojo)
    {
        int eq = snaphotDfn.getRscDfn().getResourceName().compareTo(
            otherSnapshotPojo.getSnaphotDfn().getRscDfn().getResourceName());
        if (eq == 0)
        {
            eq = snaphotDfn.getSnapshotName().compareTo(otherSnapshotPojo.getSnaphotDfn().getSnapshotName());
        }
        return eq;
    }

    @Override
    public RscLayerDataApi getLayerData()
    {
        return layerData;
    }
}
