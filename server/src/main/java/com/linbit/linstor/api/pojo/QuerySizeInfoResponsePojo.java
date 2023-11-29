package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.apis.StorPoolApi;

import java.util.List;

public class QuerySizeInfoResponsePojo
{
    private final long maxVlmSize;
    private final long availableSize;
    private final long capacity;
    private final List<StorPoolApi> nextSpawnSpList;

    public QuerySizeInfoResponsePojo(
        long maxVlmSizeRef,
        long availableSizeRef,
        long capacityRef,
        List<StorPoolApi> nextSpawnSpListRef

    )
    {
        maxVlmSize = maxVlmSizeRef;
        availableSize = availableSizeRef;
        capacity = capacityRef;
        nextSpawnSpList = nextSpawnSpListRef;
    }

    public long getMaxVlmSize()
    {
        return maxVlmSize;
    }

    public long getAvailableSize()
    {
        return availableSize;
    }

    public long getCapacity()
    {
        return capacity;
    }

    public List<StorPoolApi> nextSpawnSpList()
    {
        return nextSpawnSpList;
    }
}
