package com.linbit.linstor.core.apis;

import java.util.Map;
import java.util.UUID;

public interface StorPoolDefinitionApi
{
    UUID getUuid();
    String getName();
    Map<String, String> getProps();
}
