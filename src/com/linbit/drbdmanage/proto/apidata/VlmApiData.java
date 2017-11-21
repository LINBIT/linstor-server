package com.linbit.drbdmanage.proto.apidata;

import java.util.HashMap;
import java.util.Map;

import com.linbit.drbdmanage.Volume.VlmApi;
import com.linbit.drbdmanage.proto.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.drbdmanage.proto.VlmOuterClass.Vlm;

public class VlmApiData implements VlmApi
{
    private Vlm vlm;

    public VlmApiData(Vlm vlm)
    {
        this.vlm = vlm;
    }

    @Override
    public String getStorPoolName()
    {
        return vlm.getStorPoolName();
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
    public Map<String, String> getVlmProps()
    {
        Map<String, String> ret = new HashMap<>();
        for (LinStorMapEntry entry : vlm.getVlmPropsList())
        {
            ret.put(entry.getKey(), entry.getValue());
        }
        return ret;
    }
}
