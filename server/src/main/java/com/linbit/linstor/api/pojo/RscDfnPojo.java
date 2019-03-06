package com.linbit.linstor.api.pojo;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.utils.Pair;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class RscDfnPojo implements ResourceDefinition.RscDfnApi
{
    private final UUID uuid;
    private final String name;
    private final long flags;
    private final Map<String, String> props;
    private final List<VolumeDefinition.VlmDfnApi> vlmdfns;
    private final List<Pair<String, RscDfnLayerDataApi>> layerData;

    public RscDfnPojo(
        final UUID uuidRef,
        final String nameRef,
        final long flagsRef,
        final Map<String, String> propsRef,
        final List<VolumeDefinition.VlmDfnApi> vlmdfnsRef,
        final List<Pair<String, RscDfnLayerDataApi>> layerDataRef
    )
    {
        uuid = uuidRef;
        name = nameRef;
        flags = flagsRef;
        props = propsRef;
        vlmdfns = vlmdfnsRef;
        layerData = layerDataRef;
    }

    @Override
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public String getResourceName()
    {
        return name;
    }

    @Override
    public long getFlags()
    {
        return flags;
    }

    @Override
    public Map<String, String> getProps()
    {
        return props;
    }

    @Override
    public List<VolumeDefinition.VlmDfnApi> getVlmDfnList()
    {
        return vlmdfns;
    }

    @Override
    public List<Pair<String, RscDfnLayerDataApi>> getLayerData()
    {
        return layerData;
    }
}
