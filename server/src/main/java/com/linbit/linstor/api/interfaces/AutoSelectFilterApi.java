package com.linbit.linstor.api.interfaces;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;

import java.util.List;
import java.util.Map;

public interface AutoSelectFilterApi
{
    Integer getReplicaCount();

    Integer getAdditionalReplicaCount();

    List<String> getNodeNameList();

    List<String> getStorPoolNameList();

    List<String> getStorPoolDisklessNameList();

    List<String> getDoNotPlaceWithRscList();

    String getDoNotPlaceWithRscRegex();

    List<String> getReplicasOnSameList();

    List<String> getReplicasOnDifferentList();

    Map<String, Integer> getXReplicasOnDifferentMap();

    List<DeviceLayerKind> getLayerStackList();

    List<DeviceProviderKind> getProviderList();

    Boolean getDisklessOnRemaining();

    List<String> skipAlreadyPlacedOnNodeNamesCheck();

    Boolean skipAlreadyPlacedOnAllNodeCheck();

    String getDisklessType();

    Map<ExtTools, ExtToolsInfo.Version> getRequiredExtTools();
}
