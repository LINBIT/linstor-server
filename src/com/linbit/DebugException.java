package com.linbit;

public class DebugException extends Exception
{
    public DebugException()
    {
    }

    public DebugException(Throwable cause)
    {
        super(cause);
    }

    public DebugException(String message)
    {
        super(message);
    }

    public DebugException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
