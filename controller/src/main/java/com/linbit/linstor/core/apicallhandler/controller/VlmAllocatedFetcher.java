package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Volume;

import java.util.List;
import java.util.Map;
import java.util.Set;

import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public interface VlmAllocatedFetcher
{
    Mono<Tuple2<Map<Volume.Key, Long>, List<ApiCallRc>>> fetchVlmAllocated(
        Set<NodeName> nodesFilter,
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    );
}
