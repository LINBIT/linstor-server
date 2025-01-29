package com.linbit.linstor.event.common;

import com.linbit.linstor.event.GenericEvent;
import com.linbit.linstor.event.LinstorTriggerableEvent;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Optional;

@Singleton
public class DonePercentageEvent
{
    private final LinstorTriggerableEvent<Optional<Float>> event;

    @Inject
    public DonePercentageEvent(GenericEvent<Optional<Float>> eventRef)
    {
        event = eventRef;
    }

    public LinstorTriggerableEvent<Optional<Float>> get()
    {
        return event;
    }
}
