package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.core.objects.ResourceGroup.RscGrpApi;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
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
    private final RscGrpApi rscGrpApi;
    private final String name;
    private final byte[] externalName;
    private final long flags;
    private final Map<String, String> props;
    private final List<VolumeDefinition.VlmDfnApi> vlmdfns;
    private final List<Pair<String, RscDfnLayerDataApi>> layerData;

    public RscDfnPojo(
        final UUID uuidRef,
        final RscGrpApi rscGrpPojoRef,
        final String nameRef,
        final byte[] externalNameRef,
        final long flagsRef,
        final Map<String, String> propsRef,
        final List<VolumeDefinition.VlmDfnApi> vlmdfnsRef,
        final List<Pair<String, RscDfnLayerDataApi>> layerDataRef
    )
    {
        uuid = uuidRef;
        rscGrpApi = rscGrpPojoRef;
        name = nameRef;
        externalName = externalNameRef;
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
    public RscGrpApi getResourceGroup()
    {
        return rscGrpApi;
    }

    @Override
    public String getResourceName()
    {
        return name;
    }

    @Override
    public byte[] getExternalName()
    {
        return externalName;
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
