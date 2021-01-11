package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nullable;

import java.util.List;

public class AutoSelectFilterPojo implements AutoSelectFilterApi
{
    private @Nullable Integer placeCount; // null only allowed for resource groups
    private @Nullable Integer additionalPlaceCount;
    private @Nullable List<String> nodeNameList;
    private @Nullable List<String> storPoolNameList;
    private @Nullable List<String> doNotPlaceWithRscList;
    private @Nullable String doNotPlaceWithRegex;
    private @Nullable List<String> replicasOnSameList;
    private @Nullable List<String> replicasOnDifferentList;
    private @Nullable List<DeviceLayerKind> layerStackList;
    private @Nullable List<DeviceProviderKind> providerList;
    private @Nullable Boolean disklessOnRemaining;
    private @Nullable List<String> skipAlreadyPlacedOnNodeNamesCheck;
    private @Nullable String disklessType;

    public AutoSelectFilterPojo(
        @Nullable Integer placeCountRef,
        @Nullable Integer additionalPlaceCountRef,
        @Nullable List<String> nodeNameListRef,
        @Nullable List<String> storPoolNameListRef,
        @Nullable List<String> doNotPlaceWithRscListRef,
        @Nullable String doNotPlaceWithRegexRef,
        @Nullable List<String> replicasOnSameListRef,
        @Nullable List<String> replicasOnDifferentListRef,
        @Nullable List<DeviceLayerKind> layerStackListRef,
        @Nullable List<DeviceProviderKind> deviceProviderKindsRef,
        @Nullable Boolean disklessOnRemainingRef,
        @Nullable List<String> skipAlreadyPlacedOnNodeNamesCheckRef,
        @Nullable String disklessTypeRef
    )
    {
        placeCount = placeCountRef;
        additionalPlaceCount = additionalPlaceCountRef;
        nodeNameList = nodeNameListRef;
        storPoolNameList = storPoolNameListRef;
        doNotPlaceWithRscList = doNotPlaceWithRscListRef;
        doNotPlaceWithRegex = doNotPlaceWithRegexRef;
        replicasOnSameList = replicasOnSameListRef;
        replicasOnDifferentList = replicasOnDifferentListRef;
        disklessOnRemaining = disklessOnRemainingRef;
        layerStackList = layerStackListRef;
        providerList = deviceProviderKindsRef;
        skipAlreadyPlacedOnNodeNamesCheck = skipAlreadyPlacedOnNodeNamesCheckRef;
        disklessType = disklessTypeRef;
    }

    public static AutoSelectFilterPojo copy(AutoSelectFilterApi api) {
        return new AutoSelectFilterPojo(
            api.getReplicaCount(),
            api.getAdditionalReplicaCount(),
            api.getNodeNameList(),
            api.getStorPoolNameList(),
            api.getDoNotPlaceWithRscList(),
            api.getDoNotPlaceWithRscRegex(),
            api.getReplicasOnSameList(),
            api.getReplicasOnDifferentList(),
            api.getLayerStackList(),
            api.getProviderList(),
            api.getDisklessOnRemaining(),
            api.skipAlreadyPlacedOnNodeNamesCheck(),
            api.getDisklessType()
        );
    }

    public static AutoSelectFilterPojo merge(
        AutoSelectFilterApi... cfgArr
    )
    {
        Integer placeCount = null;
        Integer additionalPlaceCount = null;
        List<String> replicasOnDifferentList = null;
        List<String> replicasOnSameList = null;
        String notPlaceWithRscRegex = null;
        List<String> notPlaceWithRscList = null;
        List<String> nodeNameList = null;
        List<String> storPoolNameList = null;
        List<DeviceLayerKind> layerStack = null;
        List<DeviceProviderKind> providerList = null;
        Boolean disklessOnRemaining = null;
        List<String> skipAlreadyPlacedOnNodeCheck = null;
        String disklessType = null;

        for (AutoSelectFilterApi cfgApi : cfgArr)
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
                if (disklessType == null)
                {
                    disklessType = cfgApi.getDisklessType();
                }
            }
        }

        return new AutoSelectFilterPojo(
            placeCount,
            additionalPlaceCount,
            nodeNameList,
            storPoolNameList,
            notPlaceWithRscList,
            notPlaceWithRscRegex,
            replicasOnSameList,
            replicasOnDifferentList,
            layerStack,
            providerList,
            disklessOnRemaining,
            skipAlreadyPlacedOnNodeCheck,
            disklessType
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
    public @Nullable String getDisklessType()
    {
        return disklessType;
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
}
