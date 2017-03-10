package com.linbit;

/**
 * Thrown to indicate that a service failed to start
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class SystemServiceStartException extends Exception
{
    public SystemServiceStartException()
    {
    }

    public SystemServiceStartException(String message)
    {
        super(message);
    }

    public SystemServiceStartException(Throwable cause)
    {
        super(cause);
    }

    public SystemServiceStartException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
