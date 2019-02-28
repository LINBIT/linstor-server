package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.Resource;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.interfaces.RscLayerDataPojo;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtoRscLayerUtils;
import com.linbit.linstor.proto.common.RscOuterClass;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 *
 * @author rpeinthor
 */
public class RscApiData implements Resource.RscApi
{
    private final RscOuterClass.Rsc rsc;
    private final RscLayerDataPojo layerObjectApiData;

    public RscApiData(RscOuterClass.Rsc rscRef)
    {
        rsc = rscRef;
        layerObjectApiData = ProtoRscLayerUtils.extractLayerData(rsc.getLayerObject());
    }

    @Override
    public UUID getUuid()
    {
        return rsc.hasUuid() ? UUID.fromString(rsc.getUuid()) : null;
    }

    @Override
    public String getName()
    {
        return rsc.getName();
    }

    @Override
    public UUID getNodeUuid()
    {
        return rsc.hasNodeUuid() ? UUID.fromString(rsc.getNodeUuid()) : null;
    }

    @Override
    public String getNodeName()
    {
        return rsc.getNodeName();
    }

    @Override
    public UUID getRscDfnUuid()
    {
        return rsc.hasRscDfnUuid() ? UUID.fromString(rsc.getRscDfnUuid()) : null;
    }

    @Override
    public Map<String, String> getProps()
    {
        return ProtoMapUtils.asMap(rsc.getPropsList());
    }

    @Override
    public long getFlags()
    {
        return Resource.RscFlags.fromStringList(rsc.getRscFlagsList());
    }

    @Override
    public List<? extends Volume.VlmApi> getVlmList()
    {
        return rsc.getVlmsList().stream().map(VlmApiData::new).collect(Collectors.toList());
    }

    @Override
    public Integer getLocalRscNodeId()
    {
        return rsc.getOverrideNodeId() ? rsc.getNodeId() : null;
    }

    @Override
    public RscLayerDataPojo getLayerData()
    {
        return layerObjectApiData;
    }
}
