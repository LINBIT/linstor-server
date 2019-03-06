package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.VolumeDefinition;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.List;

import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.api.protobuf.ProtoLayerUtils;
import com.linbit.linstor.proto.common.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.linstor.proto.common.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.utils.Pair;

public class VlmDfnApiData implements VlmDfnApi
{
    private final VlmDfn vlmDfn;
    private final List<Pair<String, VlmDfnLayerDataApi>> layerData;

    public VlmDfnApiData(VlmDfn vlmDfnRef)
    {
        vlmDfn = vlmDfnRef;
        layerData = ProtoLayerUtils.extractVlmDfnLayerData(vlmDfnRef.getLayerDataList());
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

    @Override
    public List<Pair<String, VlmDfnLayerDataApi>> getVlmDfnLayerData()
    {
        return layerData;
    }
}
