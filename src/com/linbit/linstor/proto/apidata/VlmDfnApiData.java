package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.List;

import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.linstor.proto.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.stateflags.FlagsHelper;

import java.util.ArrayList;

public class VlmDfnApiData implements VlmDfnApi
{
    private VlmDfn vlmDfn;

    public VlmDfnApiData(VlmDfn vlmDfnRef)
    {
        vlmDfn = vlmDfnRef;
    }

    @Override
    public Integer getVolumeNr()
    {
        Integer ret = null;
        if (vlmDfn.hasVlmNr())
        {
            ret = vlmDfn.getVlmNr();
        }
        return ret;
    }

    @Override
    public long getSize()
    {
        return vlmDfn.getVlmSize();
    }

    @Override
    public Map<String, String> getProps()
    {
        Map<String, String> ret = new HashMap<>();
        for (LinStorMapEntry entry : vlmDfn.getVlmPropsList())
        {
            ret.put(entry.getKey(), entry.getValue());
        }
        return ret;
    }

    @Override
    public Integer getMinorNr()
    {
        Integer ret = null;
        if (vlmDfn.hasVlmMinor())
        {
            ret = vlmDfn.getVlmMinor();
        }
        return ret;
    }

    @Override
    public UUID getUuid()
    {
        return UUID.fromString(vlmDfn.getVlmDfnUuid());
    }

    @Override
    public long getFlags()
    {
        return FlagsHelper.fromStringList(
            VolumeDefinition.VlmDfnFlags.class,
            vlmDfn.getVlmFlagsList()
        );
    }

    public static VlmDfn fromVlmDfnApi(final VlmDfnApi vlmDfnApi)
    {
        VlmDfn.Builder bld = VlmDfn.newBuilder();
        bld.setVlmDfnUuid(vlmDfnApi.getUuid().toString());
        if (vlmDfnApi.getVolumeNr() != null)
        {
            bld.setVlmNr(vlmDfnApi.getVolumeNr());
        }
        if (vlmDfnApi.getMinorNr() != null)
        {
            bld.setVlmMinor(vlmDfnApi.getMinorNr());
        }
        bld.setVlmSize(vlmDfnApi.getSize());
        bld.addAllVlmFlags(Volume.VlmFlags.toStringList(vlmDfnApi.getFlags()));
        bld.addAllVlmProps(ProtoMapUtils.fromMap(vlmDfnApi.getProps()));
        return bld.build();
    }

    public static List<VlmDfn> fromApiList(List<VlmDfnApi> volumedefs)
    {
        ArrayList<VlmDfn> protoVlmDfs = new ArrayList<>();
        for (VlmDfnApi vlmdfnapi : volumedefs)
        {
            protoVlmDfs.add(VlmDfnApiData.fromVlmDfnApi(vlmdfnapi));
        }
        return protoVlmDfs;
    }

    public static List<VlmDfnApi> toApiList(List<VlmDfn> volumedefs)
    {
        ArrayList<VlmDfnApi> apiVlmDfns = new ArrayList<>();
        for (VlmDfn vlmdfn : volumedefs)
        {
            apiVlmDfns.add(new VlmDfnApiData(vlmdfn));
        }
        return apiVlmDfns;
    }
}
