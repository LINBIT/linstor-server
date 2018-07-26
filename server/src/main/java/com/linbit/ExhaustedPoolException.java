package com.linbit;

/**
 * Throws to indicate that a pool of resources does not have any
 * remaining free capacity
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
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
