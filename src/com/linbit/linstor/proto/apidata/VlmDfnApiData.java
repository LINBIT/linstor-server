package com.linbit.linstor.proto.apidata;

import java.util.HashMap;
import java.util.Map;

import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.linstor.proto.VlmDfnOuterClass.VlmDfn;

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
}
