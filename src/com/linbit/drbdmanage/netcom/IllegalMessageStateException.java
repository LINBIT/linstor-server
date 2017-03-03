package com.linbit.drbdmanage.netcom;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class IllegalMessageStateException extends Exception
{
    public IllegalMessageStateException()
    {
    }

    public IllegalMessageStateException(String message)
    {
        super(message);
    }

    public IllegalMessageStateException(Exception cause)
    {
        super(cause);
    }

    public IllegalMessageStateException(String message, Exception cause)
    {
        super(message, cause);
    }
}
