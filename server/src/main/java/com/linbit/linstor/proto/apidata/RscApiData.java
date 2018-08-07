package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.Resource;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.proto.RscOuterClass;

/**
 *
 * @author rpeinthor
 */
public class RscApiData
{
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

    private RscApiData()
    {
    }
}
