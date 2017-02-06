package com.linbit;

/**
 * Throws to indicate that a pool of resources does not have any
 * remaining free capacity
 *
 * @author raltnoeder
 */
public class ExhaustedPoolException extends Exception
{
    public ExhaustedPoolException()
    {
    }

    public ExhaustedPoolException(String message)
    {
        super(message);
    }
}
