package com.linbit.linstor.core.apis;

import java.util.Map;
import java.util.UUID;

public interface NodeConnectionApi
{
    UUID getUuid();
    String getLocalNodeName();
    NodeApi getOtherNodeApi();
    Map<String, String> getProps();
}
