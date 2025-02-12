package com.linbit.linstor.core.apis;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.EffectivePropertiesPojo;
import com.linbit.linstor.core.objects.Resource;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface ResourceApi
{
    @Nullable
    UUID getUuid();
    String getName();

    @Nullable
    UUID getNodeUuid();
    String getNodeName();

    @Nullable
    UUID getRscDfnUuid();
    Optional<Date> getCreateTimestamp();
    Map<String, String> getProps();
    long getFlags();
    List<? extends VolumeApi> getVlmList();
    RscLayerDataApi getLayerData();

    @Nullable
    EffectivePropertiesPojo getEffectivePropsPojo();
    default boolean isDRBDDiskless()
    {
       return (getFlags() &
           (Resource.Flags.DRBD_DISKLESS.flagValue |
               Resource.Flags.TIE_BREAKER.flagValue)) != 0;
    }
}
