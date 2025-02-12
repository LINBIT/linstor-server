package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StorPoolPojo implements Comparable<StorPoolPojo>, StorPoolApi
{
    @JsonIgnore
    private final @Nullable UUID storPoolUuid;
    @JsonIgnore
    private final @Nullable UUID nodeUuid;
    @JsonIgnore
    private final @Nullable String nodeName;
    private final String storPoolName;
    @JsonIgnore
    private final @Nullable UUID storPoolDfnUuid;
    private final DeviceProviderKind deviceProviderKind;
    @JsonIgnore
    private final @Nullable Map<String, String> storPoolProps;
    @JsonIgnore
    private final @Nullable Map<String, String> storPoolDfnProps;
    @JsonIgnore
    private final @Nullable Map<String, String> storPoolStaticTraits;
    @JsonIgnore
    private final @Nullable Long fullSyncId;
    @JsonIgnore
    private final @Nullable Long updateId;
    @JsonIgnore
    private final @Nullable String freeSpaceManagerName;
    @JsonIgnore
    private final Optional<Long> freeSpace;
    @JsonIgnore
    private final Optional<Long> totalSpace;
    @JsonIgnore
    private final double oversubscriptionRatio;
    @JsonIgnore
    private final @Nullable Double maxFreeCapacityOversubscriptionRatio;
    @JsonIgnore
    private final @Nullable Double maxTotalCapacityOversubscriptionRatio;
    @JsonIgnore
    private final @Nullable ApiCallRc reports;
    @JsonIgnore
    private final @Nullable Boolean isPmem;
    @JsonIgnore
    private final @Nullable Boolean isVDO;
    @JsonIgnore
    private final @Nullable boolean externalLocking;

    public StorPoolPojo(
        final @Nullable UUID storPoolUuidRef,
        final @Nullable UUID nodeUuidRef,
        final @Nullable String nodeNameRef,
        final String storPoolNameRef,
        final @Nullable UUID storPoolDfnUuidRef,
        final DeviceProviderKind deviceProviderKindRef,
        final @Nullable Map<String, String> storPoolPropsRef,
        final @Nullable Map<String, String> storPoolDfnPropsRef,
        final @Nullable Map<String, String> storPoolStaticTraitsRef,
        final @Nullable Long fullSyncIdRef,
        final @Nullable Long updateIdRef,
        final @Nullable String freeSpaceManagerNameRef,
        final Optional<Long> freeSpaceRef,
        final Optional<Long> totalSpaceRef,
        final double oversubscriptionRatioRef,
        final @Nullable Double maxFreeCapacityOversubscriptionRatioRef,
        final @Nullable Double maxTotalCapacityOversubscriptionRatioRef,
        final @Nullable ApiCallRc reportsRef,
        final @Nullable Boolean isPmemRef,
        final @Nullable Boolean isVDORef,
        final boolean isExternalLockingRef
    )
    {
        storPoolUuid = storPoolUuidRef;
        nodeUuid = nodeUuidRef;
        nodeName = nodeNameRef;
        storPoolName = storPoolNameRef;
        storPoolDfnUuid = storPoolDfnUuidRef;
        deviceProviderKind = deviceProviderKindRef;
        storPoolProps = storPoolPropsRef;
        storPoolDfnProps = storPoolDfnPropsRef;
        storPoolStaticTraits = storPoolStaticTraitsRef;
        fullSyncId = fullSyncIdRef;
        updateId = updateIdRef;
        freeSpaceManagerName = freeSpaceManagerNameRef;
        freeSpace = freeSpaceRef;
        totalSpace = totalSpaceRef;
        oversubscriptionRatio = oversubscriptionRatioRef;
        maxFreeCapacityOversubscriptionRatio = maxFreeCapacityOversubscriptionRatioRef;
        maxTotalCapacityOversubscriptionRatio = maxTotalCapacityOversubscriptionRatioRef;
        reports = reportsRef;
        isPmem = isPmemRef;
        isVDO = isVDORef;
        externalLocking = isExternalLockingRef;
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public StorPoolPojo(
        @JsonProperty("storPoolName") final String storPoolNameRef,
        @JsonProperty("deviceProviderKind") final DeviceProviderKind deviceProviderKindRef
    )
    {
        storPoolUuid = null;
        nodeUuid = null;
        nodeName = null;
        storPoolName = storPoolNameRef;
        storPoolDfnUuid = null;
        deviceProviderKind = deviceProviderKindRef;
        storPoolProps = null;
        storPoolDfnProps = null;
        storPoolStaticTraits = null;
        fullSyncId = null;
        updateId = null;
        freeSpaceManagerName = null;
        freeSpace = Optional.empty();
        totalSpace = Optional.empty();
        oversubscriptionRatio = LinStor.OVERSUBSCRIPTION_RATIO_UNKOWN;
        maxFreeCapacityOversubscriptionRatio = null;
        maxTotalCapacityOversubscriptionRatio = null;
        reports = null;
        isPmem = null;
        isVDO = null;

        externalLocking = false;
    }

    @Override
    public @Nullable UUID getStorPoolUuid()
    {
        return storPoolUuid;
    }

    @Override
    public @Nullable UUID getNodeUuid()
    {
        return nodeUuid;
    }

    @Override
    public @Nullable String getNodeName()
    {
        return nodeName;
    }

    @Override
    public String getStorPoolName()
    {
        return storPoolName;
    }

    @Override
    public @Nullable UUID getStorPoolDfnUuid()
    {
        return storPoolDfnUuid;
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return deviceProviderKind;
    }

    @Override
    public @Nullable String getFreeSpaceManagerName()
    {
        return freeSpaceManagerName;
    }

    @Override
    public Optional<Long> getTotalCapacity()
    {
        return totalSpace;
    }

    @Override
    public Optional<Long> getFreeCapacity()
    {
        return freeSpace;
    }

    @Override
    public double getOversubscriptionRatio()
    {
        return oversubscriptionRatio;
    }

    @Override
    public @Nullable Double getMaxFreeCapacityOversubscriptionRatio()
    {
        return maxFreeCapacityOversubscriptionRatio;
    }

    @Override
    public @Nullable Double getMaxTotalCapacityOversubscriptionRatio()
    {
        return maxTotalCapacityOversubscriptionRatio;
    }

    @Override
    public @Nullable Map<String, String> getStorPoolProps()
    {
        return storPoolProps;
    }

    @Override
    public @Nullable Map<String, String> getStorPoolDfnProps()
    {
        return storPoolDfnProps;
    }

    @Override
    public @Nullable Map<String, String> getStorPoolStaticTraits()
    {
        return storPoolStaticTraits;
    }

    @Override
    public int compareTo(StorPoolPojo otherStorPoolPojo)
    {
        return storPoolName.compareTo(otherStorPoolPojo.storPoolName);
    }

    public @Nullable Long getFullSyncId()
    {
        return fullSyncId;
    }

    public @Nullable Long getUpdateId()
    {
        return updateId;
    }

    @Override
    public @Nullable Boolean isPmem()
    {
        return isPmem;
    }

    @Override
    public @Nullable Boolean isVDO()
    {
        return isVDO;
    }

    @Override
    public @Nullable boolean isExternalLocking()
    {
        return externalLocking;
    }

    @Override
    public @Nullable ApiCallRc getReports()
    {
        return reports;
    }
}
