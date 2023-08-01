package com.linbit.linstor.api;

/**
 * A specialized pair class for use as a return value from methods which generate a value and responses.
 */
public class ApiCallRcWith<T>
{
    private final ApiCallRc apiCallRc;

    private final T value;

    public ApiCallRcWith(ApiCallRc apiCallRcRef, T valueRef)
    {
        apiCallRc = apiCallRcRef;
        value = valueRef;
    }

    public boolean hasApiCallRc()
    {
        return apiCallRc != null && !apiCallRc.isEmpty();
    }

    public ApiCallRc getApiCallRc()
    {
        return apiCallRc;
    }

    public T getValue()
    {
        return value;
    }

    public T extractApiCallRc(ApiCallRcImpl apiCallRcTarget)
    {
        apiCallRcTarget.addEntries(apiCallRc);
        return value;
    }
}
