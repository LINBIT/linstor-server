package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.protobuf.ProtoLayerUtils;
import com.linbit.linstor.proto.common.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.common.RscDfnOuterClass;
import com.linbit.linstor.proto.common.VlmDfnOuterClass.VlmDfn;
import com.linbit.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class RscDfnApiData implements ResourceDefinition.RscDfnApi
{
    private final RscDfnOuterClass.RscDfn rscDfn;
    private final List<Pair<String, RscDfnLayerDataApi>> layerData;

    public RscDfnApiData(RscDfnOuterClass.RscDfn rscDfnRef)
    {
        rscDfn = rscDfnRef;
        layerData = ProtoLayerUtils.extractRscDfnLayerData(rscDfnRef);
    }

    @Override
    public UUID getUuid()
    {
        UUID uuid = null;
        if (rscDfn.hasRscDfnUuid())
        {
            uuid = UUID.fromString(rscDfn.getRscDfnUuid());
        }
        return uuid;
    }

    @Override
    public String getResourceName()
    {
        return rscDfn.getRscName();
    }

    @Override
    public long getFlags()
    {
        return ResourceDefinition.RscDfnFlags.fromStringList(rscDfn.getRscDfnFlagsList());
    }

    @Override
    public Map<String, String> getProps()
    {
        Map<String, String> ret = new HashMap<>();
        for (LinStorMapEntryOuterClass.LinStorMapEntry entry : rscDfn.getRscDfnPropsList())
        {
            ret.put(entry.getKey(), entry.getValue());
        }
        return ret;
    }

    @Override
    public List<VolumeDefinition.VlmDfnApi> getVlmDfnList()
    {
        List<VolumeDefinition.VlmDfnApi> ret = new ArrayList<>();
        for (VlmDfn vlmDfn : rscDfn.getVlmDfnsList())
        {
            ret.add(new VlmDfnApiData(vlmDfn));
        }
        return ret;
    }

    @Override
    public List<Pair<String, RscDfnLayerDataApi>> getLayerData()
    {
        return layerData;
    }
}
