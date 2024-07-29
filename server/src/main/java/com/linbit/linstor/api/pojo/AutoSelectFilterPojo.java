package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AutoSelectFilterPojo implements AutoSelectFilterApi
{
    // Dont forget to update / regenerate hashcode and equals if you create new fields here!

    private @Nullable Integer placeCount; // null only allowed for resource groups
    private @Nullable Integer additionalPlaceCount;
    private @Nullable List<String> nodeNameList;
    private @Nullable List<String> storPoolNameList;
    private @Nullable List<String> storPoolDisklessNameList;
    private @Nullable List<String> doNotPlaceWithRscList;
    private @Nullable String doNotPlaceWithRegex;
    private @Nullable List<String> replicasOnSameList;
    private @Nullable List<String> replicasOnDifferentList;
    private @Nullable Map<String, Integer> xReplicasOnDifferentMap;
    private @Nullable List<DeviceLayerKind> layerStackList;
    private @Nullable List<DeviceProviderKind> providerList;
    private @Nullable Boolean disklessOnRemaining;
    private @Nullable List<String> skipAlreadyPlacedOnNodeNamesCheck;
    private @Nullable Boolean skipAlreadyPlacedOnAllNodeCheck;
    private @Nullable String disklessType;
    private @Nullable Map<ExtTools, Version> requiredExtTools;

    public AutoSelectFilterPojo(
        @Nullable Integer placeCountRef,
        @Nullable Integer additionalPlaceCountRef,
        @Nullable List<String> nodeNameListRef,
        @Nullable List<String> storPoolNameListRef,
        @Nullable List<String> storPoolDisklessNameListRef,
        @Nullable List<String> doNotPlaceWithRscListRef,
        @Nullable String doNotPlaceWithRegexRef,
        @Nullable List<String> replicasOnSameListRef,
        @Nullable List<String> replicasOnDifferentListRef,
        @Nullable Map<String, Integer> xReplicasOnDifferentMapRef,
        @Nullable List<DeviceLayerKind> layerStackListRef,
        @Nullable List<DeviceProviderKind> deviceProviderKindsRef,
        @Nullable Boolean disklessOnRemainingRef,
        @Nullable List<String> skipAlreadyPlacedOnNodeNamesCheckRef,
        @Nullable Boolean skipAlreadyPlacedOnAllNodeCheckRef,
        @Nullable String disklessTypeRef,
        @Nullable Map<ExtTools, Version> requiredExtToolsRef
    )
    {
        placeCount = placeCountRef;
        additionalPlaceCount = additionalPlaceCountRef;
        nodeNameList = nodeNameListRef;
        storPoolNameList = storPoolNameListRef;
        storPoolDisklessNameList = storPoolDisklessNameListRef;
        doNotPlaceWithRscList = doNotPlaceWithRscListRef;
        doNotPlaceWithRegex = doNotPlaceWithRegexRef;
        replicasOnSameList = replicasOnSameListRef;
        replicasOnDifferentList = replicasOnDifferentListRef;
        xReplicasOnDifferentMap = xReplicasOnDifferentMapRef;
        disklessOnRemaining = disklessOnRemainingRef;
        layerStackList = layerStackListRef;
        providerList = deviceProviderKindsRef;
        skipAlreadyPlacedOnNodeNamesCheck = skipAlreadyPlacedOnNodeNamesCheckRef;
        skipAlreadyPlacedOnAllNodeCheck = skipAlreadyPlacedOnAllNodeCheckRef;
        disklessType = disklessTypeRef;
        requiredExtTools = requiredExtToolsRef;
    }

    public static AutoSelectFilterPojo copy(AutoSelectFilterApi api) {
        return new AutoSelectFilterPojo(
            api.getReplicaCount(),
            api.getAdditionalReplicaCount(),
            api.getNodeNameList(),
            api.getStorPoolNameList(),
            api.getStorPoolDisklessNameList(),
            api.getDoNotPlaceWithRscList(),
            api.getDoNotPlaceWithRscRegex(),
            api.getReplicasOnSameList(),
            api.getReplicasOnDifferentList(),
            api.getXReplicasOnDifferentMap(),
            api.getLayerStackList(),
            api.getProviderList(),
            api.getDisklessOnRemaining(),
            api.skipAlreadyPlacedOnNodeNamesCheck(),
            api.skipAlreadyPlacedOnAllNodeCheck(),
            api.getDisklessType(),
            api.getRequiredExtTools()
        );
    }

    public static AutoSelectFilterPojo merge(@Nullable AutoSelectFilterApi... cfgArr)
    {
        @Nullable Integer placeCount = null;
        @Nullable Integer additionalPlaceCount = null;
        @Nullable List<String> replicasOnDifferentList = null;
        @Nullable Map<String, Integer> xReplicasOnDifferentMap = null;
        @Nullable List<String> replicasOnSameList = null;
        @Nullable String notPlaceWithRscRegex = null;
        @Nullable List<String> notPlaceWithRscList = null;
        @Nullable List<String> nodeNameList = null;
        @Nullable List<String> storPoolNameList = null;
        @Nullable List<String> storPoolDisklessNameList = null;
        @Nullable List<DeviceLayerKind> layerStack = null;
        @Nullable List<DeviceProviderKind> providerList = null;
        @Nullable Boolean disklessOnRemaining = null;
        @Nullable List<String> skipAlreadyPlacedOnNodeCheck = null;
        @Nullable Boolean skipAlreadyPlacedOnAllNodeCheck = null;
        @Nullable String disklessType = null;
        @Nullable Map<ExtTools, ExtToolsInfo.Version> requiredExtTools = null;

        for (@Nullable AutoSelectFilterApi cfgApi : cfgArr)
        {
            if (cfgApi != null)
            {
                if (placeCount == null)
                {
                    placeCount = cfgApi.getReplicaCount();
                }
                if (additionalPlaceCount == null)
                {
                    additionalPlaceCount = cfgApi.getAdditionalReplicaCount();
                }
                if (replicasOnDifferentList == null)
                {
                    replicasOnDifferentList = cfgApi.getReplicasOnDifferentList();
                }
                if (xReplicasOnDifferentMap == null)
                {
                    xReplicasOnDifferentMap = cfgApi.getXReplicasOnDifferentMap();
                }
                if (replicasOnSameList == null)
                {
                    replicasOnSameList = cfgApi.getReplicasOnSameList();
                }
                if (notPlaceWithRscList == null)
                {
                    notPlaceWithRscList = cfgApi.getDoNotPlaceWithRscList();
                }
                if (notPlaceWithRscRegex == null)
                {
                    notPlaceWithRscRegex = cfgApi.getDoNotPlaceWithRscRegex();
                }
                if (storPoolNameList == null)
                {
                    storPoolNameList = cfgApi.getStorPoolNameList();
                }
                if (storPoolDisklessNameList == null)
                {
                    storPoolDisklessNameList = cfgApi.getStorPoolDisklessNameList();
                }
                if (nodeNameList == null)
                {
                    nodeNameList = cfgApi.getNodeNameList();
                }
                if (layerStack == null)
                {
                    layerStack = cfgApi.getLayerStackList();
                }
                if (providerList == null)
                {
                    providerList = cfgApi.getProviderList();
                }
                if (disklessOnRemaining == null)
                {
                    disklessOnRemaining = cfgApi.getDisklessOnRemaining();
                }
                if (skipAlreadyPlacedOnNodeCheck == null)
                {
                    skipAlreadyPlacedOnNodeCheck = cfgApi.skipAlreadyPlacedOnNodeNamesCheck();
                }
                if (skipAlreadyPlacedOnAllNodeCheck == null)
                {
                    skipAlreadyPlacedOnAllNodeCheck = cfgApi.skipAlreadyPlacedOnAllNodeCheck();
                }
                if (disklessType == null)
                {
                    disklessType = cfgApi.getDisklessType();
                }
                if (requiredExtTools == null)
                {
                    requiredExtTools = cfgApi.getRequiredExtTools();
                }
            }
        }

        return new AutoSelectFilterPojo(
            placeCount,
            additionalPlaceCount,
            nodeNameList,
            storPoolNameList,
            storPoolDisklessNameList,
            notPlaceWithRscList,
            notPlaceWithRscRegex,
            replicasOnSameList,
            replicasOnDifferentList,
            xReplicasOnDifferentMap,
            layerStack,
            providerList,
            disklessOnRemaining,
            skipAlreadyPlacedOnNodeCheck,
            skipAlreadyPlacedOnAllNodeCheck,
            disklessType,
            requiredExtTools
        );
    }

    @Override
    public @Nullable Integer getReplicaCount()
    {
        return placeCount;
    }

    @Override
    public @Nullable Integer getAdditionalReplicaCount()
    {
        return additionalPlaceCount;
    }

    @Override
    public @Nullable List<String> getNodeNameList()
    {
        return nodeNameList;
    }

    @Override
    public @Nullable List<String> getStorPoolNameList()
    {
        return storPoolNameList;
    }

    @Override
    public @Nullable List<String> getStorPoolDisklessNameList()
    {
        return storPoolDisklessNameList;
    }

    @Override
    public @Nullable List<String> getDoNotPlaceWithRscList()
    {
        return doNotPlaceWithRscList;
    }

    @Override
    public @Nullable String getDoNotPlaceWithRscRegex()
    {
        return doNotPlaceWithRegex;
    }

    @Override
    public @Nullable List<String> getReplicasOnSameList()
    {
        return replicasOnSameList;
    }

    @Override
    public @Nullable List<String> getReplicasOnDifferentList()
    {
        return replicasOnDifferentList;
    }

    @Override
    public @Nullable Map<String, Integer> getXReplicasOnDifferentMap()
    {
        return xReplicasOnDifferentMap;
    }

    @Override
    public @Nullable List<DeviceLayerKind> getLayerStackList()
    {
        return layerStackList;
    }

    @Override
    public @Nullable List<DeviceProviderKind> getProviderList()
    {
        return providerList;
    }

    @Override
    public @Nullable Boolean getDisklessOnRemaining()
    {
        return disklessOnRemaining;
    }

    @Override
    public @Nullable List<String> skipAlreadyPlacedOnNodeNamesCheck()
    {
        return skipAlreadyPlacedOnNodeNamesCheck;
    }

    @Override
    public @Nullable Boolean skipAlreadyPlacedOnAllNodeCheck()
    {
        return skipAlreadyPlacedOnAllNodeCheck;
    }

    @Override
    public @Nullable String getDisklessType()
    {
        return disklessType;
    }

    @Override
    public @Nullable Map<ExtTools, Version> getRequiredExtTools()
    {
        return requiredExtTools;
    }

    public void setPlaceCount(@Nullable Integer placeCountRef)
    {
        placeCount = placeCountRef;
    }

    public void setAdditionalPlaceCount(@Nullable Integer additionalPlaceCountRef)
    {
        additionalPlaceCount = additionalPlaceCountRef;
    }

    public void setNodeNameList(@Nullable List<String> nodeNameListRef)
    {
        nodeNameList = nodeNameListRef;
    }

    public void setStorPoolNameList(@Nullable List<String> storPoolNameListRef)
    {
        storPoolNameList = storPoolNameListRef;
    }

    public void setDoNotPlaceWithRscList(@Nullable List<String> doNotPlaceWithRscListRef)
    {
        doNotPlaceWithRscList = doNotPlaceWithRscListRef;
    }

    public void setDoNotPlaceWithRegex(@Nullable String doNotPlaceWithRegexRef)
    {
        doNotPlaceWithRegex = doNotPlaceWithRegexRef;
    }

    public void setReplicasOnSameList(@Nullable List<String> replicasOnSameListRef)
    {
        replicasOnSameList = replicasOnSameListRef;
    }

    public void setReplicasOnDifferentList(@Nullable List<String> replicasOnDifferentListRef)
    {
        replicasOnDifferentList = replicasOnDifferentListRef;
    }

    public void setLayerStackList(@Nullable List<DeviceLayerKind> layerStackListRef)
    {
        layerStackList = layerStackListRef;
    }

    public void setProviderList(@Nullable List<DeviceProviderKind> providerListRef)
    {
        providerList = providerListRef;
    }

    public void setDisklessOnRemaining(@Nullable Boolean disklessOnRemainingRef)
    {
        disklessOnRemaining = disklessOnRemainingRef;
    }

    public void setSkipAlreadyPlacedOnNodeNamesCheck(@Nullable List<String> skipAlreadyPlacedOnNodeNamesCheckRef)
    {
        skipAlreadyPlacedOnNodeNamesCheck = skipAlreadyPlacedOnNodeNamesCheckRef;
    }

    public void setDisklessType(@Nullable String disklessTypeRef)
    {
        disklessType = disklessTypeRef;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
            additionalPlaceCount,
            disklessOnRemaining,
            disklessType,
            doNotPlaceWithRegex,
            doNotPlaceWithRscList,
            layerStackList,
            nodeNameList,
            placeCount,
            providerList,
            replicasOnDifferentList,
            replicasOnSameList,
            requiredExtTools,
            skipAlreadyPlacedOnAllNodeCheck,
            skipAlreadyPlacedOnNodeNamesCheck,
            storPoolDisklessNameList,
            storPoolNameList
        );
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!(obj instanceof AutoSelectFilterPojo))
        {
            return false;
        }
        AutoSelectFilterPojo other = (AutoSelectFilterPojo) obj;
        return Objects.equals(additionalPlaceCount, other.additionalPlaceCount) &&
            Objects.equals(disklessOnRemaining, other.disklessOnRemaining) &&
            Objects.equals(disklessType, other.disklessType) &&
            Objects.equals(doNotPlaceWithRegex, other.doNotPlaceWithRegex) &&
            Objects.equals(doNotPlaceWithRscList, other.doNotPlaceWithRscList) &&
            Objects.equals(layerStackList, other.layerStackList) &&
            Objects.equals(nodeNameList, other.nodeNameList) &&
            Objects.equals(placeCount, other.placeCount) &&
            Objects.equals(providerList, other.providerList) &&
            Objects.equals(replicasOnDifferentList, other.replicasOnDifferentList) &&
            Objects.equals(replicasOnSameList, other.replicasOnSameList) &&
            Objects.equals(requiredExtTools, other.requiredExtTools) &&
            Objects.equals(skipAlreadyPlacedOnAllNodeCheck, other.skipAlreadyPlacedOnAllNodeCheck) &&
            Objects.equals(skipAlreadyPlacedOnNodeNamesCheck, other.skipAlreadyPlacedOnNodeNamesCheck) &&
            Objects.equals(storPoolDisklessNameList, other.storPoolDisklessNameList) &&
            Objects.equals(storPoolNameList, other.storPoolNameList);
    }

}
