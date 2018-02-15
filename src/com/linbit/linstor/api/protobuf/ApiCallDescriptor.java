package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiType;

public class ApiCallDescriptor
{
    private final Class<? extends ApiCall> clazz;
    private final String name;
    private final String description;

    public ApiCallDescriptor(ApiType apiType, Class<? extends ApiCall> clazzRef)
    {
        clazz = clazzRef;
        name = apiType.getName(clazzRef);
        description = apiType.getDescription(clazzRef);
    }

    public Class<? extends ApiCall> getClazz()
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
}
