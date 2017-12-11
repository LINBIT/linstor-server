package com.linbit.linstor.proto.apidata;

import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;

import com.linbit.linstor.Volume.VlmApi;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.linstor.proto.VlmOuterClass.Vlm;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class VlmApiData implements VlmApi
{
    private Vlm vlm;

    public VlmApiData(Vlm vlm)
    {
        this.vlm = vlm;
    }

    @Override
    public UUID getVlmUuid()
    {
        if (vlm.hasVlmUuid())
            return UUID.nameUUIDFromBytes(vlm.getVlmUuid().toByteArray());
        return null;
    }

    @Override
    public UUID getVlmDfnUuid()
    {
        if (vlm.hasVlmDfnUuid())
            return UUID.nameUUIDFromBytes(vlm.getVlmDfnUuid().toByteArray());
        return null;
    }

    @Override
    public String getStorPoolName()
    {
        return vlm.getStorPoolName();
    }

    @Override
    public UUID getStorPoolUuid()
    {
        if (vlm.hasStorPoolUuid())
            return UUID.nameUUIDFromBytes(vlm.getStorPoolUuid().toByteArray());
        return null;
    }

    @Override
    public String getBlockDevice()
    {
        return vlm.getBlockDevice();
    }

    @Override
    public String getMetaDisk()
    {
        return vlm.getMetaDisk();
    }

    @Override
    public int getVlmNr()
    {
        return vlm.getVlmNr();
    }

    @Override
    public long getFlags()
    {
        return vlm.getVlmFlags();
    }

    @Override
    public Map<String, String> getVlmProps()
    {
        Map<String, String> ret = new HashMap<>();
        for (LinStorMapEntry entry : vlm.getVlmPropsList())
        {
            ret.put(entry.getKey(), entry.getValue());
        }
        return ret;
    }

    public static Vlm toVlmProto(final VlmApi vlmApi)
    {
        Vlm.Builder builder = Vlm.newBuilder();
        builder.setVlmUuid(ByteString.copyFrom(vlmApi.getVlmUuid().toString().getBytes()));
        builder.setVlmDfnUuid(ByteString.copyFrom(vlmApi.getVlmDfnUuid().toString().getBytes()));
        builder.setStorPoolName(vlmApi.getStorPoolName());
        builder.setStorPoolUuid(ByteString.copyFrom(vlmApi.getStorPoolUuid().toString().getBytes()));
        builder.setVlmNr(vlmApi.getVlmNr());
        builder.setBlockDevice(vlmApi.getBlockDevice());
        builder.setMetaDisk(vlmApi.getMetaDisk());
        builder.setVlmFlags(vlmApi.getFlags());
        builder.addAllVlmProps(BaseProtoApiCall.fromMap(vlmApi.getVlmProps()));

        return builder.build();
    }

    public static List<Vlm> toVlmProtoList(final List<VlmApi> volumedefs)
    {
        ArrayList<Vlm> protoVlm = new ArrayList<>();
        for(VlmApi vlmapi : volumedefs)
        {
            protoVlm.add(VlmApiData.toVlmProto(vlmapi));
        }
        return protoVlm;
    }

    public static List<VlmApi> toApiList(final List<Vlm> volumedefs)
    {
        ArrayList<VlmApi> apiVlms = new ArrayList<>();
        for(Vlm vlm : volumedefs)
        {
            apiVlms.add(new VlmApiData(vlm));
        }
        return apiVlms;
    }
}
