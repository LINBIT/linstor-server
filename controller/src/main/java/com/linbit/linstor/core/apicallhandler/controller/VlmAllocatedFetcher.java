package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.ApiCallRc;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface VlmAllocatedFetcher
{
    Mono<Tuple2<Map<Volume.Key, Long>, List<ApiCallRc>>> fetchVlmAllocated(
        Set<NodeName> nodesFilter,
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    );
}
