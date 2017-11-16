package com.linbit.drbdmanage.api.pojo;

import java.util.Map;
import java.util.UUID;

public class StorPoolPojo
{
    private final UUID storPoolUuid;
    private final UUID nodeUuid;
    private final String storPoolName;
    private final UUID storPoolDfnUuid;
    private final String driver;
    private final Map<String, String> storPoolProps;
    private final Map<String, String> storPoolDfnProps;

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
        this.storPoolName = storPoolName;
        this.storPoolDfnUuid = storPoolDfnUuid;
        this.driver = driver;
        this.storPoolProps = storPoolProps;
        this.storPoolDfnProps = storPoolDfnProps;
    }

    public UUID getStorPoolUuid()
    {
        return storPoolUuid;
    }

    public UUID getNodeUuid()
    {
        return nodeUuid;
    }

    public String getStorPoolName()
    {
        return storPoolName;
    }

    public UUID getStorPoolDfnUuid()
    {
        return storPoolDfnUuid;
    }

    public String getDriver()
    {
        return driver;
    }

    public Map<String, String> getStorPoolProps()
    {
        return storPoolProps;
    }

    public Map<String, String> getStorPoolDfnProps()
    {
        return storPoolDfnProps;
    }


}
