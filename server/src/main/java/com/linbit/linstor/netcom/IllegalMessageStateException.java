package com.linbit.linstor.netcom;

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

    public IllegalMessageStateException(Throwable cause)
    {
        super(cause);
    }

    public IllegalMessageStateException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
