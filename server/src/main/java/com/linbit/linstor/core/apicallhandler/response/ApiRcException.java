package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;

import java.util.stream.Collectors;

public class ApiRcException extends ApiException
{
    private final ApiCallRc apiCallRc;

    private final boolean hasContext;

    public ApiRcException(ApiCallRc.RcEntry rcEntryRef)
    {
        this(rcEntryRef, null);
    }

    public ApiRcException(ApiCallRc.RcEntry rcEntryRef, Throwable throwableRef)
    {
        this(ApiCallRcImpl.singletonApiCallRc(rcEntryRef), throwableRef, false);
    }

    public ApiRcException(ApiCallRc apiCallRcRef)
    {
        this(apiCallRcRef, null, false);
    }

    public ApiRcException(ApiCallRc apiCallRcRef, Throwable throwable, boolean hasContextRef)
    {
        super(
            apiCallRcRef.getEntries().stream()
                .map(ApiCallRc.RcEntry::getMessage)
                .collect(Collectors.joining("; ")),
            throwable
        );
        apiCallRc = apiCallRcRef;
        hasContext = hasContextRef;
    }

    public ApiCallRc getApiCallRc()
    {
        return apiCallRc;
    }

    public boolean hasContext()
    {
        return hasContext;
    }
}
