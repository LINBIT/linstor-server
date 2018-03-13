package com.linbit.linstor.api;

import java.lang.annotation.Annotation;

public interface ApiType
{
    String getBasePackageName();

    Class<? extends Annotation> getRequiredAnnotation();

    String getName(Class<?> apiCall);

    String getDescription(Class<?> apiCall);

    boolean requiresAuth(Class<?> apiCall);
}
