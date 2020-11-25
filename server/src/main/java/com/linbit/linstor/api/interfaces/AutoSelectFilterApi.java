package com.linbit.linstor.api.interfaces;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.List;

public interface AutoSelectFilterApi
{
    Integer getReplicaCount();

    Integer getAdditionalReplicaCount();

    List<String> getNodeNameList();

    List<String> getStorPoolNameList();

    List<String> getDoNotPlaceWithRscList();

    String getDoNotPlaceWithRscRegex();

    List<String> getReplicasOnSameList();

    List<String> getReplicasOnDifferentList();

    List<DeviceLayerKind> getLayerStackList();

    List<DeviceProviderKind> getProviderList();

    Boolean getDisklessOnRemaining();

    List<String> skipAlreadyPlacedOnNodeNamesCheck();

    String getDisklessType();
}
