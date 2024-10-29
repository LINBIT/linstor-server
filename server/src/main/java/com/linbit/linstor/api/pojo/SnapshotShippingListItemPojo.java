package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.SnapshotApi;
import com.linbit.linstor.core.apis.SnapshotDefinitionApi;
import com.linbit.linstor.core.apis.SnapshotShippingListItemApi;
import com.linbit.linstor.core.apis.SnapshotVolumeDefinitionApi;
import com.linbit.utils.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Deprecated(forRemoval = true)
public class SnapshotShippingListItemPojo implements SnapshotShippingListItemApi
{
    private final SnapshotDefinitionApi snapDfnApi;
    private final String sourceNodeName;
    private final String targetNodeName;
    private final String shippingStatus;

    public SnapshotShippingListItemPojo(
        SnapshotDefinitionApi snapDfnApiRef,
        String sourceNodeNameRef,
        String targetNodeNameRef,
        String shippingStatusRef
    )
    {
        snapDfnApi = snapDfnApiRef;
        sourceNodeName = sourceNodeNameRef;
        targetNodeName = targetNodeNameRef;
        shippingStatus = shippingStatusRef;
    }

    @Override
    public ResourceDefinitionApi getRscDfn()
    {
        return snapDfnApi.getRscDfn();
    }

    @Override
    public UUID getUuid()
    {
        return snapDfnApi.getUuid();
    }

    @Override
    public String getSnapshotName()
    {
        return snapDfnApi.getSnapshotName();
    }

    @Override
    public long getFlags()
    {
        return snapDfnApi.getFlags();
    }

    @Override
    public Map<String, String> getSnapDfnProps()
    {
        return snapDfnApi.getSnapDfnProps();
    }

    @Override
    public Map<String, String> getRscDfnProps()
    {
        return snapDfnApi.getRscDfnProps();
    }

    @Override
    public List<SnapshotVolumeDefinitionApi> getSnapshotVlmDfnList()
    {
        return snapDfnApi.getSnapshotVlmDfnList();
    }

    @Override
    public List<Pair<String, RscDfnLayerDataApi>> getLayerData()
    {
        return snapDfnApi.getLayerData();
    }

    @Override
    public List<SnapshotApi> getSnapshots()
    {
        return snapDfnApi.getSnapshots();
    }

    @Override
    public String getSourceNodeName()
    {
        return sourceNodeName;
    }

    @Override
    public String getTargetNodeName()
    {
        return targetNodeName;
    }

    @Override
    public String getShippingStatus()
    {
        return shippingStatus;
    }

    @Override
    public List<String> getNodeNames()
    {
        return Arrays.asList(sourceNodeName, targetNodeName);
    }
}
