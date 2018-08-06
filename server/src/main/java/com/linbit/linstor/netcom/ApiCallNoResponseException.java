package com.linbit.linstor.netcom;

public class ApiCallNoResponseException extends RuntimeException
{
    public ApiCallNoResponseException()
    {
        super("No response from API call");
    }
}
