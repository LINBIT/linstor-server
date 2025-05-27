package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.apis.ResourceConnectionApi;

import java.util.Map;
import java.util.UUID;

public class RscConnPojo implements ResourceConnectionApi
{
    private final UUID uuid;
    private final String sourceNodeName;
    private final String targetNodeName;
    private final String rscName;
    private final Map<String, String> props;
    private final long flags;
    private final @Nullable Integer drbdProxyPortSource;
    private final @Nullable Integer drbdProxyPortTarget;

    public RscConnPojo(
        UUID uuidRef,
        String sourceNodeNameRef,
        String targetNodeNameRef,
        String rscNameRef,
        Map<String, String> propsRef,
        long flagRef,
        @Nullable Integer drbdProxyPortSourceRef,
        @Nullable Integer drbdProxyPortTargetRef
    )
    {
        uuid = uuidRef;
        sourceNodeName = sourceNodeNameRef;
        targetNodeName = targetNodeNameRef;
        rscName = rscNameRef;
        props = propsRef;
        flags = flagRef;
        drbdProxyPortSource = drbdProxyPortSourceRef;
        drbdProxyPortTarget = drbdProxyPortTargetRef;
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

    @Override
    public @Nullable Integer getDrbdProxyPortSource()
    {
        return drbdProxyPortSource;
    }

    @Override
    public @Nullable Integer getDrbdProxyPortTarget()
    {
        return drbdProxyPortTarget;
    }
}
