package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.Volume;
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

    public VlmApiData(Vlm vlmRef)
    {
        vlm = vlmRef;
    }

    @Override
    public UUID getVlmUuid()
    {
        UUID vlmUuid = null;
        if (vlm.hasVlmUuid())
        {
            vlmUuid = UUID.fromString(vlm.getVlmUuid());
        }
        return null;
    }

    @Override
    public UUID getVlmDfnUuid()
    {
        UUID vlmDfnUuid = null;
        if (vlm.hasVlmDfnUuid())
        {
            vlmDfnUuid = UUID.fromString(vlm.getVlmDfnUuid());
        }
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
        UUID vlmUuid = null;
        if (vlm.hasStorPoolUuid())
        {
            vlmUuid = UUID.fromString(vlm.getStorPoolUuid());
        }
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
    public int getVlmMinorNr()
    {
        return vlm.getVlmMinorNr();
    }

    @Override
    public long getFlags()
    {
        return Volume.VlmFlags.fromStringList(vlm.getVlmFlagsList());
    }

    @Override
    public String getStorDriverSimpleClassName()
    {
        return vlm.getStorPoolDriverName();
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
        builder.setVlmUuid(vlmApi.getVlmUuid().toString());
        builder.setVlmDfnUuid(vlmApi.getVlmDfnUuid().toString());
        builder.setStorPoolName(vlmApi.getStorPoolName());
        builder.setStorPoolUuid(vlmApi.getStorPoolUuid().toString());
        builder.setVlmNr(vlmApi.getVlmNr());
        builder.setVlmMinorNr(vlmApi.getVlmMinorNr());
        if (vlmApi.getBlockDevice() != null)
        {
            builder.setBlockDevice(vlmApi.getBlockDevice());
        }
        if (vlmApi.getMetaDisk() != null)
        {
            builder.setMetaDisk(vlmApi.getMetaDisk());
        }
        builder.addAllVlmFlags(Volume.VlmFlags.toStringList(vlmApi.getFlags()));
        builder.addAllVlmProps(BaseProtoApiCall.fromMap(vlmApi.getVlmProps()));

        return builder.build();
    }

    public static List<Vlm> toVlmProtoList(final List<? extends VlmApi> volumedefs)
    {
        ArrayList<Vlm> protoVlm = new ArrayList<>();
        for (VlmApi vlmapi : volumedefs)
        {
            protoVlm.add(VlmApiData.toVlmProto(vlmapi));
        }
        return protoVlm;
    }

    public static List<VlmApi> toApiList(final List<Vlm> volumedefs)
    {
        ArrayList<VlmApi> apiVlms = new ArrayList<>();
        for (Vlm vlm : volumedefs)
        {
            apiVlms.add(new VlmApiData(vlm));
        }
        return apiVlms;
    }
}
