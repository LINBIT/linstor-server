package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.apis.ResourceGroupApi;
import com.linbit.linstor.core.apis.VolumeGroupApi;

import com.linbit.linstor.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RscGrpPojo implements ResourceGroupApi
{
    private final @Nullable UUID uuid;
    private final String rscGrpName;
    private final String description;
    private final Map<String, String> rscDfnPropsMap;
    private final List<VolumeGroupApi> vlmGrpList;
    private final @Nullable AutoSelectFilterApi autoSelectFilter;
    private final @Nullable Short peerSlots;

    public RscGrpPojo(
        @Nullable UUID uuidRef,
        String rscGrpNameStrRef,
        String descriptionRef,
        Map<String, String> rscDfnPropsRef,
        List<VolumeGroupApi> vlmGrpListRef,
        @Nullable AutoSelectFilterApi autoSelectFilterRef,
        @Nullable Short peerSlotsRef
    )
    {
        uuid = uuidRef;
        rscGrpName = rscGrpNameStrRef;
        description = descriptionRef;
        rscDfnPropsMap = rscDfnPropsRef;
        vlmGrpList = vlmGrpListRef;
        autoSelectFilter = autoSelectFilterRef;
        peerSlots = peerSlotsRef;
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
    public List<VolumeGroupApi> getVlmGrpList()
    {
        return vlmGrpList;
    }

    @Override
    public Map<String, String> getProps()
    {
        return rscDfnPropsMap;
    }

    @Override
    public @Nullable AutoSelectFilterApi getAutoSelectFilter()
    {
        return autoSelectFilter;
    }

    @Override
    public @Nullable Short getPeerSlots()
    {
        return peerSlots;
    }
}
