package com.linbit.drbdmanage.api;

import java.lang.annotation.Annotation;

import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;

public enum ApiType
{
    PROTOBUF (BaseProtoApiCall.class, ProtobufApiCall.class);

    private final String basePackageName;
    private final Class<? extends Annotation> requiredAnnotation;

    private ApiType(
        final Class<? extends BaseApiCall> baseClass,
        final Class<? extends Annotation> annot)
    {
        this.basePackageName = baseClass.getPackage().getName();
        this.requiredAnnotation = annot;
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
