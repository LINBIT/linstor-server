package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.SpaceInfo;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface FreeCapacityFetcher
{
    Mono<Map<StorPool.Key, Long>> fetchThinFreeCapacities(Set<NodeName> nodesFilter);

    Mono<Map<StorPool.Key, Tuple2<SpaceInfo, List<ApiCallRc>>>> fetchThinFreeSpaceInfo(Set<NodeName> nodesFilter);
}
