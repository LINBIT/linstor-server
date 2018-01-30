package com.linbit.linstor.api.pojo;

import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;

import java.util.List;
import java.util.Map;
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

    public StorPoolPojo(
        final UUID storPoolUuid,
        final UUID nodeUuid,
        final String nodeName,
        final String storPoolName,
        final UUID storPoolDfnUuid,
        final String driver,
        final Map<String, String> storPoolProps,
        final Map<String, String> storPoolDfnProps,
        final List<Volume.VlmApi> vlms,
        final Map<String, String> storPoolStaticTraits,
        final Long fullSyncId,
        final Long updateId
    )
    {
        this.storPoolUuid = storPoolUuid;
        this.nodeUuid = nodeUuid;
        this.nodeName = nodeName;
        this.storPoolName = storPoolName;
        this.storPoolDfnUuid = storPoolDfnUuid;
        this.driver = driver;
        this.storPoolProps = storPoolProps;
        this.storPoolDfnProps = storPoolDfnProps;
        this.vlms = vlms;
        this.storPoolStaticTraits = storPoolStaticTraits;
        this.fullSyncId = fullSyncId;
        this.updateId = updateId;
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
    public Map<String, String> getStorPoolProps()
    {
        return storPoolProps;
    }

    public Map<String, String> getStorPoolDfnProps()
    {
        return storPoolDfnProps;
    }

    @Override
    public List<Volume.VlmApi> getVlmList() {
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
