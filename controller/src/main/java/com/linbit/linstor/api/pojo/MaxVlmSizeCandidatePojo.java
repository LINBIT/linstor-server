package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.objects.StorPoolDefinition.StorPoolDfnApi;

import java.util.List;

public class MaxVlmSizeCandidatePojo
{
    private final StorPoolDfnApi storPoolDfnApi;

    private final boolean allThin;

    private final List<String> nodeNames;

    private final long maxVlmSize;

    public MaxVlmSizeCandidatePojo(
        StorPoolDfnApi storPoolDfnApiRef,
        boolean allThinRef,
        List<String> nodeNamesRef,
        long capacityRef
    )
    {
        storPoolDfnApi = storPoolDfnApiRef;
        allThin = allThinRef;
        nodeNames = nodeNamesRef;
        maxVlmSize = capacityRef;
    }

    public StorPoolDfnApi getStorPoolDfnApi()
    {
        return storPoolDfnApi;
    }

    public boolean areAllThin()
    {
        return allThin;
    }

    public List<String> getNodeNames()
    {
        return nodeNames;
    }

    public long getMaxVlmSize()
    {
        return maxVlmSize;
    }
}
