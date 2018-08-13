package com.linbit.linstor.event.common;

import com.linbit.linstor.event.GenericEvent;
import com.linbit.linstor.event.LinstorTriggerableEvent;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class VolumeDiskStateEvent
{
    private final LinstorTriggerableEvent<String> event;

    @Inject
    public VolumeDiskStateEvent(GenericEvent<String> eventRef)
    {
        event = eventRef;
    }

    public LinstorTriggerableEvent<String> get()
    {
        return event;
    }
}
