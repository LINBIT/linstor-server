package com.linbit.linstor.api;

import com.linbit.linstor.annotation.Nullable;

/**
 * A specialized pair class for use as a return value from methods which generate a value and responses.
 */
public class ApiCallRcWith<T>
{
    private final @Nullable ApiCallRc apiCallRc;

    private final @Nullable T value;

    public ApiCallRcWith(@Nullable ApiCallRc apiCallRcRef, @Nullable T valueRef)
    {
        apiCallRc = apiCallRcRef;
        value = valueRef;
    }

    public boolean hasApiCallRc()
    {
        return apiCallRc != null && !apiCallRc.isEmpty();
    }

    public @Nullable ApiCallRc getApiCallRc()
    {
        return apiCallRc;
    }

    public @Nullable T getValue()
    {
        return value;
    }

    public @Nullable T extractApiCallRc(ApiCallRcImpl apiCallRcTarget)
    {
        apiCallRcTarget.addEntries(apiCallRc);
        return value;
    }
}
