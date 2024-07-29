package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.EffectivePropertiesPojo;
import com.linbit.linstor.api.protobuf.ProtoLayerUtils;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.proto.common.RscOuterClass;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 *
 * @author rpeinthor
 */
public class RscApiData implements ResourceApi
{
    private final RscOuterClass.Rsc rsc;
    private final RscLayerDataApi layerObjectApiData;

    public RscApiData(RscOuterClass.Rsc rscRef, long fullSyncId, long updateId)
    {
        rsc = rscRef;
        layerObjectApiData = ProtoLayerUtils.extractRscLayerData(
            rsc.getLayerObject(),
            fullSyncId,
            updateId
        );
    }

    @Override
    public @Nullable UUID getUuid()
    {
        return rsc.hasUuid() ? UUID.fromString(rsc.getUuid()) : null;
    }

    @Override
    public String getName()
    {
        return rsc.getName();
    }

    @Override
    public @Nullable UUID getNodeUuid()
    {
        return rsc.hasNodeUuid() ? UUID.fromString(rsc.getNodeUuid()) : null;
    }

    @Override
    public String getNodeName()
    {
        return rsc.getNodeName();
    }

    @Override
    public @Nullable UUID getRscDfnUuid()
    {
        return rsc.hasRscDfnUuid() ? UUID.fromString(rsc.getRscDfnUuid()) : null;
    }

    @Override
    public Map<String, String> getProps()
    {
        return rsc.getPropsMap();
    }

    @Override
    public long getFlags()
    {
        return Resource.Flags.fromStringList(rsc.getRscFlagsList());
    }

    @Override
    public List<? extends VolumeApi> getVlmList()
    {
        return rsc.getVlmsList().stream().map(VlmApiData::new).collect(Collectors.toList());
    }

    @Override
    public RscLayerDataApi getLayerData()
    {
        return layerObjectApiData;
    }

    @Override
    @Nullable
    public Optional<Date> getCreateTimestamp()
    {
        return Optional.empty();
    }

    @Override
    public @Nullable EffectivePropertiesPojo getEffectivePropsPojo()
    {
        // proto does not need effectiveProps
        return null;
    }
}
