package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.api.ApiType;

import java.lang.annotation.Annotation;

public class ProtobufApiType implements ApiType
{
    @Override
    public String getBasePackageName()
    {
        return getClass().getPackage().getName();
    }

    @Override
    public Class<? extends Annotation> getRequiredAnnotation()
    {
        return ProtobufApiCall.class;
    }

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
}
