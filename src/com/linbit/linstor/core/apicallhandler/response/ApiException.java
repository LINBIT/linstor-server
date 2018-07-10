package com.linbit.linstor.core.apicallhandler.response;

public class ApiException extends RuntimeException
{
    public ApiException(String message, Throwable throwableRef)
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
