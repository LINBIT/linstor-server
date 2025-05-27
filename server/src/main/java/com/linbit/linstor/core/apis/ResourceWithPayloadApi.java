package com.linbit.linstor.core.apis;

import com.linbit.linstor.annotation.Nullable;

import java.util.List;

public interface ResourceWithPayloadApi
{
    ResourceApi getRscApi();
    List<String> getLayerStack();
    Integer getDrbdNodeId();

    @Nullable
    List<Integer> getPorts();

    @Nullable
    Integer getPortCount();
}
