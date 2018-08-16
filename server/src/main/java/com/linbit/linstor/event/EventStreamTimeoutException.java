package com.linbit.linstor.event;

public class EventStreamTimeoutException extends RuntimeException
{
    public EventStreamTimeoutException()
    {
        super("Wait for event stream timed out");
    }
}
