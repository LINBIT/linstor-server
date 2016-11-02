package com.linbit;

public class ImplementationError extends Error
{
    public ImplementationError(Throwable cause)
    {
        super(cause);
    }

    public ImplementationError(String message, Throwable cause)
    {
        super(message, cause);
    }
}
