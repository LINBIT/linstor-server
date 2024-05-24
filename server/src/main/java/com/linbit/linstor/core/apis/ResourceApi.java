package com.linbit.linstor.core.apis;

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
    UUID getUuid();
    String getName();
    UUID getNodeUuid();
    String getNodeName();
    UUID getRscDfnUuid();
    Optional<Date> getCreateTimestamp();
    Map<String, String> getProps();
    long getFlags();
    List<? extends VolumeApi> getVlmList();
    RscLayerDataApi getLayerData();
    EffectivePropertiesPojo getEffectivePropsPojo();
    default boolean isDRBDDiskless()
    {
       return (getFlags() &
           (Resource.Flags.DRBD_DISKLESS.flagValue |
               Resource.Flags.TIE_BREAKER.flagValue)) != 0;
    }
}
