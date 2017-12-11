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

    public StorPoolPojo(
        UUID storPoolUuid,
        UUID nodeUuid,
        String nodeName,
        String storPoolName,
        UUID storPoolDfnUuid,
        String driver,
        Map<String, String> storPoolProps,
        Map<String, String> storPoolDfnProps,
        List<Volume.VlmApi> vlms
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
    }

    public StorPoolPojo(
        UUID storPoolUuid,
        UUID nodeUuid,
        String storPoolName,
        UUID storPoolDfnUuid,
        String driver,
        Map<String, String> storPoolProps,
        Map<String, String> storPoolDfnProps
    )
    {
        this.storPoolUuid = storPoolUuid;
        this.nodeUuid = nodeUuid;
        this.nodeName = null;
        this.storPoolName = storPoolName;
        this.storPoolDfnUuid = storPoolDfnUuid;
        this.driver = driver;
        this.storPoolProps = storPoolProps;
        this.storPoolDfnProps = storPoolDfnProps;
        this.vlms = null;
    }

    @Override
    public UUID getStorPoolUuid()
    {
        return storPoolUuid;
    }

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
    public int compareTo(StorPoolPojo otherStorPoolPojo)
    {
        return storPoolName.compareTo(otherStorPoolPojo.storPoolName);
    }
}
