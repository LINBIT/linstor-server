package com.linbit.drbdmanage.proto.apidata;

import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.Volume.VlmApi;
import com.linbit.drbdmanage.proto.MsgCrtVlmOuterClass.Vlm;

public class VlmApiData implements VlmApi
{
    private Vlm vlm;

    public VlmApiData(Vlm vlm)
    {
        this.vlm = vlm;
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

}
