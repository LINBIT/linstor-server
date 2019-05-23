package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.proto.common.VlmGrpOuterClass;
import com.linbit.linstor.proto.common.VlmGrpOuterClass.VlmGrp;

import java.util.Map;
import java.util.UUID;

public class VlmGrpApiData implements VolumeGroup.VlmGrpApi
{
    private final VlmGrp vlmGrp;

    public VlmGrpApiData(VlmGrpOuterClass.VlmGrp vlmGrpRef)
    {
        vlmGrp = vlmGrpRef;
    }

    @Override
    public Integer getVolumeNr()
    {
        return vlmGrp.hasVlmNr() ? vlmGrp.getVlmNr() : null;
    }

    @Override
    public Map<String, String> getProps()
    {
        return vlmGrp.getVlmDfnPropsMap();
    }

    @Override
    public UUID getUUID()
    {
        UUID uuid = null;
        if (vlmGrp.hasUuid())
        {
            uuid = UUID.fromString(vlmGrp.getUuid());
        }
        return uuid;
    }

}
