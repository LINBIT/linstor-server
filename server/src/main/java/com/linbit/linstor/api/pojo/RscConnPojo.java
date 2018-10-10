package com.linbit.linstor.api.pojo;

import java.util.Map;
import java.util.UUID;

import com.linbit.linstor.ResourceConnection;

public class RscConnPojo implements ResourceConnection.RscConnApi
{
    private final UUID uuid;
    private String sourceNodeName;
    private String targetNodeName;
    private String rscName;
    private final Map<String, String> props;
    private long flags;

    public RscConnPojo(
        UUID uuidRef,
        String sourceNodeNameRef,
        String targetNodeNameRef,
        String rscNameRef,
        Map<String, String> propsRef,
        long flagRef
    )
    {
        this.uuid = uuidRef;
        this.sourceNodeName = sourceNodeNameRef;
        this.targetNodeName = targetNodeNameRef;
        this.rscName = rscNameRef;
        this.props = propsRef;
        this.flags = flagRef;
    }

    @Override
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public String getSourceNodeName()
    {
        return sourceNodeName;
    }

    @Override
    public String getTargetNodeName()
    {
        return targetNodeName;
    }

    @Override
    public String getResourceName()
    {
        return rscName;
    }

    @Override
    public Map<String, String> getProps()
    {
        return props;
    }

    @Override
    public long getFlags()
    {
        return flags;
    }
}
