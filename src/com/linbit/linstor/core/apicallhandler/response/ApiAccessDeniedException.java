package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.security.AccessDeniedException;

public class ApiAccessDeniedException extends ApiException
{
    private final String action;
    private final long retCode;

    public ApiAccessDeniedException(AccessDeniedException accDeniedExcRef, String actionRef, long retCodeRef)
    {
        super("Access denied " + actionRef, accDeniedExcRef);

        action = actionRef;
        retCode = retCodeRef;
    }

    public String getAction()
    {
        return action;
    }

    public long getRetCode()
    {
        return retCode;
    }
}
