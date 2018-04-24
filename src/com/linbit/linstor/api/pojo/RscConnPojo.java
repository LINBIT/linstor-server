package com.linbit.linstor.api.pojo;

import java.util.Map;
import java.util.UUID;

import com.linbit.linstor.ResourceConnection;

public class RscConnPojo implements ResourceConnection.RscConnApi
{
    private final UUID uuid;
    private String sourceNodeName;
    private String targetNodeName;
    private final Map<String, String> props;

    public RscConnPojo(
        UUID uuidRef,
        String sourceNodeNameRef,
        String targetNodeNameRef,
        Map<String, String> propsRef
    )
    {
        this.uuid = uuidRef;
        this.sourceNodeName = sourceNodeNameRef;
        this.targetNodeName = targetNodeNameRef;
        this.props = propsRef;
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
    public Map<String, String> getProps()
    {
        return props;
    }
}
