package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.api.ApiCallRc;

public class VlmAllocatedResult
{
    private long allocatedSize;
    private ApiCallRc apiCallRc;

    public VlmAllocatedResult(
        long allocatedSizeRef,
        ApiCallRc apiCallRcRef
    )
    {
        allocatedSize = allocatedSizeRef;
        apiCallRc = apiCallRcRef;
    }

    public long getAllocatedSize()
    {
        return allocatedSize;
    }

    public ApiCallRc getApiCallRc()
    {
        return apiCallRc;
    }

    public boolean hasErrors()
    {
        return !apiCallRc.isEmpty();
    }
}
