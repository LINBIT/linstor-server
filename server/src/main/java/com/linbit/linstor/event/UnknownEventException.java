package com.linbit.linstor.event;

public class UnknownEventException extends RuntimeException
{
    private final String eventName;

    public UnknownEventException(String eventNameRef)
    {
        super("Event '" + eventNameRef + "' not known");
        eventName = eventNameRef;
    }

    public String getEventName()
    {
        return eventName;
    }
}
