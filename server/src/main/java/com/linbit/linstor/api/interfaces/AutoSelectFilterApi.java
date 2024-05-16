package com.linbit.linstor.api.interfaces;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.utils.IndentedWriter;

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

    default String asHelpString()
    {
        return asHelpString("");
    }

    default String asHelpString(String indentRef)
    {
        IndentedWriter iw = new IndentedWriter().withIndentGlobal(indentRef);
        iw.appendIfExists("Replica count", getReplicaCount());
        iw.appendIfExists("Additional replica count", getAdditionalReplicaCount());
        iw.appendIfNotEmpty("Node name", getNodeNameList());
        iw.appendIfNotEmpty("Storage pool name", getStorPoolNameList());
        iw.appendIfNotEmpty("Storage pool diskless name", getStorPoolDisklessNameList());
        iw.appendIfNotEmpty("Do not place with resource", getDoNotPlaceWithRscList());
        iw.appendIfExists("Do not place with resource (regex)", getDoNotPlaceWithRscRegex());
        iw.appendIfNotEmpty(
            "Replicas on nodes with same property",
            "Replicas on nodes with same properties",
            getReplicasOnSameList()
        );
        iw.appendIfNotEmpty(
            "Replicas on nodes with different property",
            "Replicas on nodes with different properties",
            getReplicasOnDifferentList()
        );
        iw.appendIfNotEmpty(
            "X replicas on nodes with different property",
            "X replicas on nodes with different properties",
            getXReplicasOnDifferentMap()
        );
        iw.appendIfNotEmptyNoPlural("Layer stack", getLayerStackList());
        iw.appendIfNotEmpty("Allowed Provider", getProviderList());
        iw.appendIfExists("Diskless on remaining", getDisklessOnRemaining());
        iw.appendIfNotEmpty("Skip check if already placed on node", skipAlreadyPlacedOnNodeNamesCheck());
        iw.appendIfExists("Skip check if already placed on any node", skipAlreadyPlacedOnAllNodeCheck());
        iw.appendIfExists("Diskful/Diskless type", getDisklessType());
        iw.appendIfNotEmpty("Required external tool", getRequiredExtTools());
        return iw.toString();
    }
}
