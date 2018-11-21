package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.api.ApiType;

public class ProtobufApiType implements ApiType
{
    @Override
    public String getName(Class<?> apiCall)
    {
        return apiCall.getAnnotation(ProtobufApiCall.class).name();
    }

    @Override
    public String getDescription(Class<?> apiCall)
    {
        return apiCall.getAnnotation(ProtobufApiCall.class).description();
    }

    @Override
    public boolean requiresAuth(Class<?> apiCall)
    {
        return apiCall.getAnnotation(ProtobufApiCall.class).requiresAuth();
    }

    @Override
    public boolean transactional(Class<?> apiCall)
    {
        return apiCall.getAnnotation(ProtobufApiCall.class).transactional();
    }
}
