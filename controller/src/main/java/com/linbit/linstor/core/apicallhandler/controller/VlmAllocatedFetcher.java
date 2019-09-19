package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Volume;

import java.util.Map;
import java.util.Set;

import reactor.core.publisher.Mono;

public interface VlmAllocatedFetcher
{
    Mono<Map<Volume.Key, VlmAllocatedResult>> fetchVlmAllocated(
        Set<NodeName> nodesFilter,
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    );
}
