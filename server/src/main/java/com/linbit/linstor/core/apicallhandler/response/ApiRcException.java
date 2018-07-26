package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.api.ApiCallRc;

public class ApiRcException extends ApiException
{
    private final ApiCallRc.RcEntry rcEntry;

    private final boolean hasContext;

    public ApiRcException(ApiCallRc.RcEntry rcEntryRef, Throwable throwableRef, boolean hasContextRef)
    {
        super(rcEntryRef.getMessage(), throwableRef);
        rcEntry = rcEntryRef;
        hasContext = hasContextRef;
    }

    public ApiRcException(ApiCallRc.RcEntry rcEntryRef, Throwable throwableRef)
    {
        this(rcEntryRef, throwableRef, false);
    }

    public ApiRcException(ApiCallRc.RcEntry rcEntryRef)
    {
        this(rcEntryRef, null);
    }

    public ApiCallRc.RcEntry getRcEntry()
    {
        return rcEntry;
    }

    public boolean hasContext()
    {
        return hasContext;
    }
}
