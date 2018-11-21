package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.BaseApiCall;

public class ApiCallDescriptor
{
    private final Class<? extends BaseApiCall> clazz;
    private final String name;
    private final String description;
    private final boolean reqAuth;
    private final boolean transactional;

    public ApiCallDescriptor(ApiType apiType, Class<? extends BaseApiCall> clazzRef)
    {
        clazz = clazzRef;
        name = apiType.getName(clazzRef);
        description = apiType.getDescription(clazzRef);
        reqAuth = apiType.requiresAuth(clazzRef);
        transactional = apiType.transactional(clazzRef);
    }

    public Class<? extends BaseApiCall> getClazz()
    {
        return clazz;
    }

    public String getName()
    {
        return name;
    }

    public String getDescription()
    {
        return description;
    }

    public boolean requiresAuth()
    {
        return reqAuth;
    }

    public boolean transactional()
    {
        return transactional;
    }
}
