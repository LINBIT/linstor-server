package com.linbit.linstor.layer.drbd.drbdstate;

/**
 * Thrown to indicate that an event line supplied by DRBD could not be processed
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class EventsSourceException extends Exception
{
    public EventsSourceException()
    {

    }
    public EventsSourceException(String message)
    {
        super(message);
    }

    public EventsSourceException(String message, Exception nestedExc)
    {
        super(message, nestedExc);
    }
}
