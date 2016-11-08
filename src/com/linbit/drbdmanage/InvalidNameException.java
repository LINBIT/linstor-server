package com.linbit.drbdmanage;

/**
 * Thrown to indicate that a name or identifier is not valid
 */
public class InvalidNameException extends Exception
{
    public InvalidNameException()
    {
    }

    public InvalidNameException(String message)
    {
        super(message);
    }

    public InvalidNameException(String message, Exception nestedException)
    {
        super(message, nestedException);
    }
}
