package com.linbit.linstor.api;

public interface ApiType
{
    String getName(Class<?> apiCall);

    String getDescription(Class<?> apiCall);

    boolean requiresAuth(Class<?> apiCall);

    boolean transactional(Class<?> apiCall);
}
