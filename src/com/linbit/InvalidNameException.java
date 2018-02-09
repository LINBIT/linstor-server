package com.linbit;

/**
 * Thrown to indicate that a name or identifier is not valid
 */
public class InvalidNameException extends Exception
{
    private static final long serialVersionUID = 8630329749041929643L;

    public final String invalidName;

    public InvalidNameException(String message, String invalidNameRef)
    {
        super(message);
        invalidName = invalidNameRef;
    }
}
