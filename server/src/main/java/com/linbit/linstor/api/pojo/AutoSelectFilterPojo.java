package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

public class AutoSelectFilterPojo implements AutoSelectFilterApi
{
    private final @Nullable Integer placeCount; // null only allowed for resource groupss
    private final @Nullable String storPoolNameStr;
    private final List<String> doNotPlaceWithRscList;
    private final @Nullable String doNotPlaceWithRegex;
    private final List<String> replicasOnSameList;
    private final List<String> replicasOnDifferentList;
    private final List<DeviceLayerKind> layerStackList;
    private final List<DeviceProviderKind> providerList;

    public AutoSelectFilterPojo(
        @Nullable Integer placeCountRef,
        @Nullable String storPoolNameStrRef,
        List<String> doNotPlaceWithRscListRef,
        @Nullable String doNotPlaceWithRegexRef,
        List<String> replicasOnSameListRef,
        List<String> replicasOnDifferentListRef,
        List<DeviceLayerKind> layerStackListRef,
        List<DeviceProviderKind> deviceProviderKindsRef
    )
    {
        placeCount = placeCountRef;
        storPoolNameStr = storPoolNameStrRef;
        doNotPlaceWithRscList = doNotPlaceWithRscListRef;
        doNotPlaceWithRegex = doNotPlaceWithRegexRef;
        replicasOnSameList = replicasOnSameListRef;
        replicasOnDifferentList = replicasOnDifferentListRef;
        layerStackList = layerStackListRef == null ? Collections.emptyList() : layerStackListRef;
        providerList = deviceProviderKindsRef == null ? Collections.emptyList() : deviceProviderKindsRef;
    }

    @Override
    public Integer getReplicaCount()
    {
        return placeCount;
    }

    @Override
    public @Nullable String getStorPoolNameStr()
    {
        return storPoolNameStr;
    }

    @Override
    public List<String> getDoNotPlaceWithRscList()
    {
        return doNotPlaceWithRscList;
    }

    @Override
    public @Nullable String getDoNotPlaceWithRscRegex()
    {
        return doNotPlaceWithRegex;
    }

    @Override
    public List<String> getReplicasOnSameList()
    {
        return replicasOnSameList;
    }

    @Override
    public List<String> getReplicasOnDifferentList()
    {
        return replicasOnDifferentList;
    }

    @Override
    public List<DeviceLayerKind> getLayerStackList()
    {
        return layerStackList;
    }

    @Override
    public List<DeviceProviderKind> getProviderList()
    {
        return providerList;
    }
}
