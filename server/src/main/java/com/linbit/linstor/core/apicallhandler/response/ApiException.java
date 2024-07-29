package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.annotation.Nullable;

public class ApiException extends RuntimeException
{
    public ApiException(String message, @Nullable Throwable throwableRef)
    {
        super(message, throwableRef);
    }

    public ApiException(String message)
    {
        this(message, null);
    }

    public ApiException(Throwable throwable)
    {
        super(throwable);
    }
}
