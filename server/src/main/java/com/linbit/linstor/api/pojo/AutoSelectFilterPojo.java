package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nullable;

import java.util.List;

public class AutoSelectFilterPojo implements AutoSelectFilterApi
{
    private final @Nullable Integer placeCount; // null only allowed for resource groupss
    private final @Nullable List<String> nodeNameList;
    private final @Nullable List<String> storPoolNameList;
    private final @Nullable List<String> doNotPlaceWithRscList;
    private final @Nullable String doNotPlaceWithRegex;
    private final @Nullable List<String> replicasOnSameList;
    private final @Nullable List<String> replicasOnDifferentList;
    private final @Nullable List<DeviceLayerKind> layerStackList;
    private final @Nullable List<DeviceProviderKind> providerList;
    private final @Nullable Boolean disklessOnRemaining;
    private final @Nullable Boolean skipAlreadyPlacedOnNodeCheck;

    public AutoSelectFilterPojo(
        @Nullable Integer placeCountRef,
        @Nullable List<String> nodeNameListRef,
        @Nullable List<String> storPoolNameListRef,
        @Nullable List<String> doNotPlaceWithRscListRef,
        @Nullable String doNotPlaceWithRegexRef,
        @Nullable List<String> replicasOnSameListRef,
        @Nullable List<String> replicasOnDifferentListRef,
        @Nullable List<DeviceLayerKind> layerStackListRef,
        @Nullable List<DeviceProviderKind> deviceProviderKindsRef,
        @Nullable Boolean disklessOnRemainingRef,
        @Nullable Boolean skipAlreadyPlacedOnNodeCheckRef
    )
    {
        placeCount = placeCountRef;
        nodeNameList = nodeNameListRef;
        storPoolNameList = storPoolNameListRef;
        doNotPlaceWithRscList = doNotPlaceWithRscListRef;
        doNotPlaceWithRegex = doNotPlaceWithRegexRef;
        replicasOnSameList = replicasOnSameListRef;
        replicasOnDifferentList = replicasOnDifferentListRef;
        disklessOnRemaining = disklessOnRemainingRef;
        layerStackList = layerStackListRef;
        providerList = deviceProviderKindsRef;
        skipAlreadyPlacedOnNodeCheck = skipAlreadyPlacedOnNodeCheckRef;
    }

    public static AutoSelectFilterPojo merge(
        AutoSelectFilterApi... cfgArr
    )
    {
        Integer placeCount = null;
        List<String> replicasOnDifferentList = null;
        List<String> replicasOnSameList = null;
        String notPlaceWithRscRegex = null;
        List<String> notPlaceWithRscList = null;
        List<String> nodeNameList = null;
        List<String> storPoolNameList = null;
        List<DeviceLayerKind> layerStack = null;
        List<DeviceProviderKind> providerList = null;
        Boolean disklessOnRemaining = null;
        Boolean skipAlreadyPlacedOnNodeCheck = null;

        for (AutoSelectFilterApi cfgApi : cfgArr)
        {
            if (cfgApi != null)
            {
                if (placeCount == null)
                {
                    placeCount = cfgApi.getReplicaCount();
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
                    skipAlreadyPlacedOnNodeCheck = cfgApi.skipAlreadyPlacedOnNodeCheck();
                }
            }
        }

        return new AutoSelectFilterPojo(
            placeCount,
            nodeNameList,
            storPoolNameList,
            notPlaceWithRscList,
            notPlaceWithRscRegex,
            replicasOnSameList,
            replicasOnDifferentList,
            layerStack,
            providerList,
            disklessOnRemaining,
            skipAlreadyPlacedOnNodeCheck
        );
    }

    @Override
    public @Nullable Integer getReplicaCount()
    {
        return placeCount;
    }

    @Override
    public List<String> getNodeNameList()
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
    public @Nullable Boolean skipAlreadyPlacedOnNodeCheck()
    {
        return skipAlreadyPlacedOnNodeCheck;
    }
}
