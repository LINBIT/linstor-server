package com.linbit.linstor.api.pojo.builder;

import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

public class AutoSelectFilterBuilder
{
    private @Nullable Integer placeCount = null;
    private @Nullable Integer additionalPlaceCount = null;
    private @Nullable List<String> nodeNameList = null;
    private @Nullable List<String> storPoolNameList = null;
    private @Nullable List<String> storPoolDisklessNameList = null;
    private @Nullable List<String> doNotPlaceWithRscList = null;
    private @Nullable String doNotPlaceWithRegex = null;
    private @Nullable List<String> replicasOnSameList = null;
    private @Nullable List<String> replicasOnDifferentList = null;
    private @Nullable Map<String, Integer> xReplicasOnDifferentMap = null;
    private @Nullable List<DeviceLayerKind> layerStackList = null;
    private @Nullable List<DeviceProviderKind> deviceProviderKinds = null;
    private @Nullable Boolean disklessOnRemaining = null;
    private @Nullable List<String> skipAlreadyPlacedOnNodeNamesCheck = null;
    private @Nullable Boolean skipAlreadyPlacedOnAllNodeCheck = null;
    private @Nullable String disklessType = null;
    private @Nullable Map<ExtTools, Version> extTools = null;

    public AutoSelectFilterBuilder(AutoSelectFilterPojo pojo)
    {
        placeCount = pojo.getReplicaCount();
        additionalPlaceCount = pojo.getAdditionalReplicaCount();
        nodeNameList = pojo.getNodeNameList();
        storPoolNameList = pojo.getStorPoolNameList();
        storPoolDisklessNameList = pojo.getStorPoolDisklessNameList();
        doNotPlaceWithRscList = pojo.getDoNotPlaceWithRscList();
        doNotPlaceWithRegex = pojo.getDoNotPlaceWithRscRegex();
        replicasOnSameList = pojo.getReplicasOnSameList();
        replicasOnDifferentList = pojo.getReplicasOnDifferentList();
        xReplicasOnDifferentMap = pojo.getXReplicasOnDifferentMap();
        layerStackList = pojo.getLayerStackList();
        deviceProviderKinds = pojo.getProviderList();
        disklessOnRemaining = pojo.getDisklessOnRemaining();
        skipAlreadyPlacedOnNodeNamesCheck = pojo.skipAlreadyPlacedOnNodeNamesCheck();
        skipAlreadyPlacedOnAllNodeCheck = pojo.skipAlreadyPlacedOnAllNodeCheck();
        disklessType = pojo.getDisklessType();
        extTools = pojo.getRequiredExtTools();
    }

    public AutoSelectFilterBuilder()
    {
    }

    public AutoSelectFilterBuilder setPlaceCount(Integer placeCountRef)
    {
        placeCount = placeCountRef;
        return this;
    }

    public AutoSelectFilterBuilder setAdditionalPlaceCount(Integer additionalPlaceCountRef)
    {
        additionalPlaceCount = additionalPlaceCountRef;
        return this;
    }

    public AutoSelectFilterBuilder setNodeNameList(List<String> nodeNameListRef)
    {
        nodeNameList = nodeNameListRef;
        return this;
    }

    public AutoSelectFilterBuilder setStorPoolNameList(List<String> storPoolNameListRef)
    {
        storPoolNameList = storPoolNameListRef;
        return this;
    }

    public AutoSelectFilterBuilder setStorPoolDisklessNameList(List<String> storPoolDisklessNameListRef)
    {
        storPoolDisklessNameList = storPoolDisklessNameListRef;
        return this;
    }

    public AutoSelectFilterBuilder setDoNotPlaceWithRscList(List<String> doNotPlaceWithRscListRef)
    {
        doNotPlaceWithRscList = doNotPlaceWithRscListRef;
        return this;
    }

    public AutoSelectFilterBuilder setDoNotPlaceWithRegex(String doNotPlaceWithRegexRef)
    {
        doNotPlaceWithRegex = doNotPlaceWithRegexRef;
        return this;
    }

    public AutoSelectFilterBuilder setReplicasOnSameList(List<String> replicasOnSameListRef)
    {
        replicasOnSameList = replicasOnSameListRef;
        return this;
    }

    public AutoSelectFilterBuilder setReplicasOnDifferentList(List<String> replicasOnDifferentListRef)
    {
        replicasOnDifferentList = replicasOnDifferentListRef;
        return this;
    }

    public AutoSelectFilterBuilder setXReplicasOnDifferentMap(Map<String, Integer> xReplicasOnDifferentMapRef)
    {
        xReplicasOnDifferentMap = xReplicasOnDifferentMapRef;
        return this;
    }

    public AutoSelectFilterBuilder setLayerStackList(List<DeviceLayerKind> layerStackListRef)
    {
        layerStackList = layerStackListRef;
        return this;
    }

    public AutoSelectFilterBuilder setDeviceProviderKinds(List<DeviceProviderKind> deviceProviderKindsRef)
    {
        deviceProviderKinds = deviceProviderKindsRef;
        return this;
    }

    public AutoSelectFilterBuilder setDisklessOnRemaining(Boolean disklessOnRemainingRef)
    {
        disklessOnRemaining = disklessOnRemainingRef;
        return this;
    }

    public AutoSelectFilterBuilder setSkipAlreadyPlacedOnNodeNamesCheck(
        List<String> skipAlreadyPlacedOnNodeNamesCheckRef
    )
    {
        skipAlreadyPlacedOnNodeNamesCheck = skipAlreadyPlacedOnNodeNamesCheckRef;
        return this;
    }

    public AutoSelectFilterBuilder setSkipAlreadyPlacedOnAllNodeCheck(Boolean skipAlreadyPlacedOnAllNodeCheckRef)
    {
        skipAlreadyPlacedOnAllNodeCheck = skipAlreadyPlacedOnAllNodeCheckRef;
        return this;
    }

    public AutoSelectFilterBuilder setDisklessType(String disklessTypeRef)
    {
        disklessType = disklessTypeRef;
        return this;
    }

    public AutoSelectFilterPojo build() {
        return new AutoSelectFilterPojo(
            placeCount,
            additionalPlaceCount,
            nodeNameList,
            storPoolNameList,
            storPoolDisklessNameList,
            doNotPlaceWithRscList,
            doNotPlaceWithRegex,
            replicasOnSameList,
            replicasOnDifferentList,
            xReplicasOnDifferentMap,
            layerStackList,
            deviceProviderKinds,
            disklessOnRemaining,
            skipAlreadyPlacedOnNodeNamesCheck,
            skipAlreadyPlacedOnAllNodeCheck,
            disklessType,
            extTools
        );
    }

    public AutoSelectFilterBuilder setRequireExtTools(Map<ExtTools, Version> extToolsRef)
    {
        extTools = extToolsRef;
        return this;
    }
}
