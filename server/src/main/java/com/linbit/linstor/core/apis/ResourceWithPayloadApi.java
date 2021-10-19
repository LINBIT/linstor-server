package com.linbit.linstor.core.apis;

import java.util.List;

public interface ResourceWithPayloadApi
{
    ResourceApi getRscApi();
    List<String> getLayerStack();
    Integer getDrbdNodeId();
}
