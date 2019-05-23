package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.objects.ResourceGroup.RscGrpApi;
import com.linbit.linstor.core.objects.VolumeGroup.VlmGrpApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RscGrpPojo implements RscGrpApi
{
    private final @Nullable UUID uuid;
    private final String rscGrpName;
    private final String description;
    private final List<DeviceLayerKind> layerStack;
    private final Map<String, String> rscDfnPropsMap;
    private final List<VlmGrpApi> vlmGrpList;
    private final @Nullable AutoSelectFilterApi autoSelectFilter;

    public RscGrpPojo(
        @Nullable UUID uuidRef,
        String rscGrpNameStrRef,
        String descriptionRef,
        List<DeviceLayerKind> layerStackStrRef,
        Map<String, String> rscDfnPropsRef,
        List<VlmGrpApi> vlmGrpListRef,
        @Nullable AutoSelectFilterApi autoSelectFilterRef
    )
    {
        uuid = uuidRef;
        rscGrpName = rscGrpNameStrRef;
        description = descriptionRef;
        layerStack = layerStackStrRef;
        rscDfnPropsMap = rscDfnPropsRef;
        vlmGrpList = vlmGrpListRef;
        autoSelectFilter = autoSelectFilterRef;
    }

    @Override
    public @Nullable UUID getUuid()
    {
        return uuid;
    }

    @Override
    public String getName()
    {
        return rscGrpName;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public List<DeviceLayerKind> getLayerStack()
    {
        return layerStack;
    }

    @Override
    public List<VlmGrpApi> getVlmGrpList()
    {
        return vlmGrpList;
    }

    @Override
    public Map<String, String> getRcsDfnProps()
    {
        return rscDfnPropsMap;
    }

    @Override
    public @Nullable AutoSelectFilterApi getAutoSelectFilter()
    {
        return autoSelectFilter;
    }

}
