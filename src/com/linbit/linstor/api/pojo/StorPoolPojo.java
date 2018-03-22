package com.linbit.linstor.api.pojo;

import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class StorPoolPojo implements Comparable<StorPoolPojo>, StorPool.StorPoolApi
{
    private final UUID storPoolUuid;
    private final UUID nodeUuid;
    private final String nodeName;
    private final String storPoolName;
    private final UUID storPoolDfnUuid;
    private final String driver;
    private final Map<String, String> storPoolProps;
    private final Map<String, String> storPoolDfnProps;
    private final List<Volume.VlmApi> vlms;
    private final Map<String, String> storPoolStaticTraits;
    private final Long fullSyncId;
    private final Long updateId;
    private final Optional<Long> freeSpace;

    public StorPoolPojo(
        final UUID storPoolUuidRef,
        final UUID nodeUuidRef,
        final String nodeNameRef,
        final String storPoolNameRef,
        final UUID storPoolDfnUuidRef,
        final String driverRef,
        final Map<String, String> storPoolPropsRef,
        final Map<String, String> storPoolDfnPropsRef,
        final List<Volume.VlmApi> vlmsRef,
        final Map<String, String> storPoolStaticTraitsRef,
        final Long fullSyncIdRef,
        final Long updateIdRef,
        final Long freeSpaceRef
    )
    {
        storPoolUuid = storPoolUuidRef;
        nodeUuid = nodeUuidRef;
        nodeName = nodeNameRef;
        storPoolName = storPoolNameRef;
        storPoolDfnUuid = storPoolDfnUuidRef;
        driver = driverRef;
        storPoolProps = storPoolPropsRef;
        storPoolDfnProps = storPoolDfnPropsRef;
        vlms = vlmsRef;
        storPoolStaticTraits = storPoolStaticTraitsRef;
        fullSyncId = fullSyncIdRef;
        updateId = updateIdRef;
        freeSpace = Optional.ofNullable(freeSpaceRef);
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
    public String getDriver()
    {
        return driver;
    }

    @Override
    public Optional<Long> getFreeSpace()
    {
        return freeSpace;
    }

    @Override
    public Map<String, String> getStorPoolProps()
    {
        return storPoolProps;
    }

    public Map<String, String> getStorPoolDfnProps()
    {
        return storPoolDfnProps;
    }

    @Override
    public List<Volume.VlmApi> getVlmList()
    {
        return vlms;
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
}
