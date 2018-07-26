package com.linbit;

public class ChildProcessTimeoutException extends TimeoutException
{
    private final boolean termFlag;

    public ChildProcessTimeoutException()
    {
        termFlag = false;
    }

    public ChildProcessTimeoutException(String message)
    {
        super(message);
        termFlag = false;
    }

    public ChildProcessTimeoutException(String message, boolean terminated)
    {
        super(message);
        termFlag = terminated;
    }

    public ChildProcessTimeoutException(boolean terminated)
    {
        termFlag = terminated;
    }

    public boolean isTerminated()
    {
        return termFlag;
    }
}
