package com.linbit;

public class DebugException extends Exception
{
    public DebugException()
    {
    }

    public DebugException(Exception cause)
    {
        super(cause);
    }

    public DebugException(String message)
    {
        super(message);
    }

    public DebugException(String message, Exception cause)
    {
        super(message, cause);
    }
}
