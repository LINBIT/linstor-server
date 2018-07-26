package com.linbit.fsevent;

import com.linbit.TimeoutException;

public class FsWatchTimeoutException extends TimeoutException
{
    public FsWatchTimeoutException()
    {
    }

    public FsWatchTimeoutException(String message)
    {
        super(message);
    }
}
