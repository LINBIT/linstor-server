package com.linbit.linstor.api.interfaces;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.List;

public interface AutoSelectFilterApi
{
    int getPlaceCount();

    String getStorPoolNameStr();

    List<String> getNotPlaceWithRscList();

    String getNotPlaceWithRscRegex();

    List<String> getReplicasOnSameList();

    List<String> getReplicasOnDifferentList();

    List<DeviceLayerKind> getLayerStackList();

    List<DeviceProviderKind> getProviderList();
}
