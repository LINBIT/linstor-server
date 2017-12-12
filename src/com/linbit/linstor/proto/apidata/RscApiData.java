/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linbit.linstor.proto.apidata;

import com.google.protobuf.ByteString;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.RscOuterClass;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class RscApiData implements Resource.RscApi {
    RscOuterClass.Rsc resource;

    public RscApiData(RscOuterClass.Rsc refResource)
    {
        resource = refResource;
    }

    @Override
    public UUID getUuid() {
        if (resource.hasUuid())
            return UUID.nameUUIDFromBytes(resource.getUuid().toByteArray());
        return null;
    }

    @Override
    public String getName() {
        return resource.getName();
    }

    @Override
    public UUID getNodeUuid() {
        if (resource.hasNodeUuid())
            return UUID.nameUUIDFromBytes(resource.getNodeUuid().toByteArray());
        return null;
    }

    @Override
    public String getNodeName() {
        return resource.getNodeName();
    }

    @Override
    public UUID getRscDfnUuid() {
        if (resource.hasRscDfnUuid())
            return UUID.nameUUIDFromBytes(resource.getRscDfnUuid().toByteArray());
        return null;
    }

    @Override
    public Map<String, String> getProps() {
        Map<String, String> ret = new HashMap<>();
        for (LinStorMapEntryOuterClass.LinStorMapEntry entry : resource.getPropsList())
        {
            ret.put(entry.getKey(), entry.getValue());
        }
        return ret;
    }

    @Override
    public List<? extends Volume.VlmApi> getVlmList() {
        return VlmApiData.toApiList(resource.getVlmsList());
    }

    public static RscOuterClass.Rsc toRscProto(final Resource.RscApi apiResource)
    {
        RscOuterClass.Rsc.Builder rscBld = RscOuterClass.Rsc.newBuilder();

        rscBld.setName(apiResource.getName());
        rscBld.setUuid(ByteString.copyFrom(apiResource.getUuid().toString().getBytes()));
        rscBld.setNodeName(apiResource.getNodeName());
        rscBld.setNodeUuid(ByteString.copyFrom(apiResource.getNodeUuid().toString().getBytes()));
        rscBld.setRscDfnUuid(ByteString.copyFrom(apiResource.getRscDfnUuid().toString().getBytes()));
        rscBld.addAllProps(BaseProtoApiCall.fromMap(apiResource.getProps()));
        rscBld.addAllVlms(VlmApiData.toVlmProtoList(apiResource.getVlmList()));

        return rscBld.build();
    }

}
