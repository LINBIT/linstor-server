package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StorPoolPojo implements Comparable<StorPoolPojo>, StorPoolApi
{
    @JsonIgnore
    private final UUID storPoolUuid;
    @JsonIgnore
    private final UUID nodeUuid;
    @JsonIgnore
    private final String nodeName;
    private final String storPoolName;
    @JsonIgnore
    private final UUID storPoolDfnUuid;
    private final DeviceProviderKind deviceProviderKind;
    @JsonIgnore
    private final Map<String, String> storPoolProps;
    @JsonIgnore
    private final Map<String, String> storPoolDfnProps;
    @JsonIgnore
    private final Map<String, String> storPoolStaticTraits;
    @JsonIgnore
    private final Long fullSyncId;
    @JsonIgnore
    private final Long updateId;
    @JsonIgnore
    private final String freeSpaceManagerName;
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
    private final ApiCallRc reports;
    @JsonIgnore
    private final Boolean supportsSnapshots;
    @JsonIgnore
    private final Boolean isPmem;
    @JsonIgnore
    private final Boolean isVDO;
    @JsonIgnore
    private final Boolean externalLocking;

    public StorPoolPojo(
        final UUID storPoolUuidRef,
        final UUID nodeUuidRef,
        final String nodeNameRef,
        final String storPoolNameRef,
        final UUID storPoolDfnUuidRef,
        final DeviceProviderKind deviceProviderKindRef,
        final Map<String, String> storPoolPropsRef,
        final Map<String, String> storPoolDfnPropsRef,
        final Map<String, String> storPoolStaticTraitsRef,
        final Long fullSyncIdRef,
        final Long updateIdRef,
        final String freeSpaceManagerNameRef,
        final Optional<Long> freeSpaceRef,
        final Optional<Long> totalSpaceRef,
        final double oversubscriptionRatioRef,
        final @Nullable Double maxFreeCapacityOversubscriptionRatioRef,
        final @Nullable Double maxTotalCapacityOversubscriptionRatioRef,
        final ApiCallRc reportsRef,
        final Boolean supportsSnapshotsRef,
        final Boolean isPmemRef,
        final Boolean isVDORef,
        final Boolean isExternalLockingRef
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
        supportsSnapshots = supportsSnapshotsRef;
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
        supportsSnapshots = null;
        isPmem = null;
        isVDO = null;
        externalLocking = null;
    }

    @Override
    public UUID getStorPoolUuid()
    {
        return storPoolUuid;
    }

    @Override
    public UUID getNodeUuid()
    {
        return nodeUuid;
    }

    @Override
    public String getNodeName()
    {
        return nodeName;
    }

    @Override
    public String getStorPoolName()
    {
        return storPoolName;
    }

    @Override
    public UUID getStorPoolDfnUuid()
    {
        return storPoolDfnUuid;
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return deviceProviderKind;
    }

    @Override
    public String getFreeSpaceManagerName()
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
    public Map<String, String> getStorPoolProps()
    {
        return storPoolProps;
    }

    @Override
    public Map<String, String> getStorPoolDfnProps()
    {
        return storPoolDfnProps;
    }

    @Override
    public Map<String, String> getStorPoolStaticTraits()
    {
        return storPoolStaticTraits;
    }

    @Override
    public int compareTo(StorPoolPojo otherStorPoolPojo)
    {
        return storPoolName.compareTo(otherStorPoolPojo.storPoolName);
    }

    public long getFullSyncId()
    {
        return fullSyncId;
    }

    public long getUpdateId()
    {
        return updateId;
    }

    @Override
    public Boolean supportsSnapshots()
    {
        return supportsSnapshots;
    }

    @Override
    public Boolean isPmem()
    {
        return isPmem;
    }

    @Override
    public Boolean isVDO() {
        return isVDO;
    }

    @Override
    public Boolean isExternalLocking()
    {
        return externalLocking;
    }

    @Override
    public ApiCallRc getReports()
    {
        return reports;
    }
}
