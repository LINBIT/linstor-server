package com.linbit.drbdmanage.proto.apidata;

import java.util.HashMap;
import java.util.Map;

import com.linbit.drbdmanage.VolumeDefinition.VlmDfnApi;
import com.linbit.drbdmanage.proto.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.drbdmanage.proto.MsgCrtVlmDfnOuterClass.VlmDfn;

public class VlmDfnApiData implements VlmDfnApi
{
    private VlmDfn vlmDfn;

    public VlmDfnApiData(VlmDfn vlmDfnRef)
    {
        vlmDfn = vlmDfnRef;
    }

    @Override
    public int getVolumeNr()
    {
        return vlmDfn.getVlmNr();
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
    public int getMinorNr()
    {
        return vlmDfn.getVlmMinor();
    }
}
