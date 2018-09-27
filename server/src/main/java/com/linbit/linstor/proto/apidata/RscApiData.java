package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.Resource;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.linstor.proto.RscOuterClass;

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

    public RscApiData(RscOuterClass.Rsc rscRef)
    {
        rsc = rscRef;
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

    public static RscOuterClass.Rsc toRscProto(final Resource.RscApi apiResource)
    {
        RscOuterClass.Rsc.Builder rscBld = RscOuterClass.Rsc.newBuilder();

        rscBld.setName(apiResource.getName());
        rscBld.setUuid(apiResource.getUuid().toString());
        rscBld.setNodeName(apiResource.getNodeName());
        rscBld.setNodeUuid(apiResource.getNodeUuid().toString());
        rscBld.setRscDfnUuid(apiResource.getRscDfnUuid().toString());
        rscBld.addAllRscFlags(Resource.RscFlags.toStringList(apiResource.getFlags()));
        rscBld.addAllProps(ProtoMapUtils.fromMap(apiResource.getProps()));
        rscBld.addAllVlms(VlmApiData.toVlmProtoList(apiResource.getVlmList()));
        rscBld.setNodeId(apiResource.getLocalRscNodeId());

        return rscBld.build();
    }
}
