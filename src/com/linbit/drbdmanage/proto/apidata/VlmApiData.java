package com.linbit.drbdmanage.proto.apidata;

import java.util.Map;

import com.linbit.drbdmanage.Volume.VlmApi;
import com.linbit.drbdmanage.proto.MsgCrtRscOuterClass.Vlm;

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
        return vlm.getVlmPropsMap();
    }

}
