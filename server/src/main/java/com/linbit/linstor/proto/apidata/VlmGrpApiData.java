package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.apis.VolumeGroupApi;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.proto.common.VlmGrpOuterClass;
import com.linbit.linstor.proto.common.VlmGrpOuterClass.VlmGrp;

import java.util.Map;
import java.util.UUID;

public class VlmGrpApiData implements VolumeGroupApi
{
    private final VlmGrp vlmGrp;

    public VlmGrpApiData(VlmGrpOuterClass.VlmGrp vlmGrpRef)
    {
        vlmGrp = vlmGrpRef;
    }

    @Override
    public @Nullable Integer getVolumeNr()
    {
        return vlmGrp.hasVlmNr() ? vlmGrp.getVlmNr() : null;
    }

    @Override
    public Map<String, String> getProps()
    {
        return vlmGrp.getVlmDfnPropsMap();
    }

    @Override
    public long getFlags()
    {
        return VolumeGroup.Flags.fromStringList(vlmGrp.getFlagsList());
    }

    @Override
    public @Nullable UUID getUUID()
    {
        UUID uuid = null;
        if (vlmGrp.hasUuid())
        {
            uuid = UUID.fromString(vlmGrp.getUuid());
        }
        return uuid;
    }

}
