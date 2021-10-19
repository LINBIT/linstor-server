package com.linbit.linstor.core.apis;

import java.util.Map;
import java.util.UUID;

public interface ResourceConnectionApi
{
    UUID getUuid();
    String getSourceNodeName();
    String getTargetNodeName();
    String getResourceName();
    Map<String, String> getProps();
    long getFlags();
    Integer getPort();
}
