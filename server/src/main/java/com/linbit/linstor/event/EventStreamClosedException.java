package com.linbit.linstor.event;

public class EventStreamClosedException extends RuntimeException
{
    public EventStreamClosedException()
    {
        super("Event stream closed unexpectedly");
    }
}
