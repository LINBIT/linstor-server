package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.apis.StorPoolDefinitionApi;

import java.util.List;

public class MaxVlmSizeCandidatePojo
{
    private final StorPoolDefinitionApi storPoolDfnApi;

    private final boolean allThin;

    private final List<String> nodeNames;

    private final long maxVlmSize;

    public MaxVlmSizeCandidatePojo(
        StorPoolDefinitionApi storPoolDfnApiRef,
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

    public StorPoolDefinitionApi getStorPoolDfnApi()
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
