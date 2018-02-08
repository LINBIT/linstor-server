package com.linbit.linstor.api;

import java.lang.annotation.Annotation;

import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;

public enum ApiType
{
    PROTOBUF(
        BaseProtoApiCall.class.getPackage().getName(),
        ProtobufApiCall.class
    );

    private final String basePackageName;
    private final Class<? extends Annotation> requiredAnnotation;

    ApiType(
        final String basePackageNameRef,
        final Class<? extends Annotation> annotRef
    )
    {
        basePackageName = basePackageNameRef;
        requiredAnnotation = annotRef;
    }

    public String getBasePackageName()
    {
        return basePackageName;
    }

    public Class<? extends Annotation> getRequiredAnnotation()
    {
        return requiredAnnotation;
    }
}
