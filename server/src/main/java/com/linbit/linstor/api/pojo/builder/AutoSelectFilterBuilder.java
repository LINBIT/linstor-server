package com.linbit.linstor.api.pojo.builder;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;

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
    private @Nullable Integer drbdPortCount = null;

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
        drbdPortCount = pojo.getDrbdPortCount();
    }

    public AutoSelectFilterBuilder()
    {
    }

    public AutoSelectFilterBuilder setPlaceCount(@Nullable Integer placeCountRef)
    {
        placeCount = placeCountRef;
        return this;
    }

    public AutoSelectFilterBuilder setAdditionalPlaceCount(@Nullable Integer additionalPlaceCountRef)
    {
        additionalPlaceCount = additionalPlaceCountRef;
        return this;
    }

    public AutoSelectFilterBuilder setNodeNameList(@Nullable List<String> nodeNameListRef)
    {
        nodeNameList = nodeNameListRef;
        return this;
    }

    public AutoSelectFilterBuilder setStorPoolNameList(@Nullable List<String> storPoolNameListRef)
    {
        storPoolNameList = storPoolNameListRef;
        return this;
    }

    public AutoSelectFilterBuilder setStorPoolDisklessNameList(@Nullable List<String> storPoolDisklessNameListRef)
    {
        storPoolDisklessNameList = storPoolDisklessNameListRef;
        return this;
    }

    public AutoSelectFilterBuilder setDoNotPlaceWithRscList(@Nullable List<String> doNotPlaceWithRscListRef)
    {
        doNotPlaceWithRscList = doNotPlaceWithRscListRef;
        return this;
    }

    public AutoSelectFilterBuilder setDoNotPlaceWithRegex(@Nullable String doNotPlaceWithRegexRef)
    {
        doNotPlaceWithRegex = doNotPlaceWithRegexRef;
        return this;
    }

    public AutoSelectFilterBuilder setReplicasOnSameList(@Nullable List<String> replicasOnSameListRef)
    {
        replicasOnSameList = replicasOnSameListRef;
        return this;
    }

    public AutoSelectFilterBuilder setReplicasOnDifferentList(@Nullable List<String> replicasOnDifferentListRef)
    {
        replicasOnDifferentList = replicasOnDifferentListRef;
        return this;
    }

    public AutoSelectFilterBuilder setXReplicasOnDifferentMap(@Nullable Map<String, Integer> xReplicasOnDifferentMapRef)
    {
        xReplicasOnDifferentMap = xReplicasOnDifferentMapRef;
        return this;
    }

    public AutoSelectFilterBuilder setLayerStackList(@Nullable List<DeviceLayerKind> layerStackListRef)
    {
        layerStackList = layerStackListRef;
        return this;
    }

    public AutoSelectFilterBuilder setDeviceProviderKinds(@Nullable List<DeviceProviderKind> deviceProviderKindsRef)
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
        @Nullable List<String> skipAlreadyPlacedOnNodeNamesCheckRef
    )
    {
        skipAlreadyPlacedOnNodeNamesCheck = skipAlreadyPlacedOnNodeNamesCheckRef;
        return this;
    }

    public AutoSelectFilterBuilder setSkipAlreadyPlacedOnAllNodeCheck(
        @Nullable Boolean skipAlreadyPlacedOnAllNodeCheckRef
    )
    {
        skipAlreadyPlacedOnAllNodeCheck = skipAlreadyPlacedOnAllNodeCheckRef;
        return this;
    }

    public AutoSelectFilterBuilder setDisklessType(@Nullable String disklessTypeRef)
    {
        disklessType = disklessTypeRef;
        return this;
    }

    public AutoSelectFilterBuilder setDrbdPortCount(@Nullable Integer drbdPortCountRef)
    {
        drbdPortCount = drbdPortCountRef;
        return this;
    }

    public AutoSelectFilterPojo build()
    {
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
            extTools,
            drbdPortCount
        );
    }

    public AutoSelectFilterBuilder setRequireExtTools(@Nullable Map<ExtTools, Version> extToolsRef)
    {
        extTools = extToolsRef;
        return this;
    }
}
