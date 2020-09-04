package com.linbit.linstor.event.common;

import com.linbit.linstor.event.GenericEvent;
import com.linbit.linstor.event.LinstorTriggerableEvent;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ResourceStateEvent
{
    private final LinstorTriggerableEvent<ResourceState> event;

    @Inject
    public ResourceStateEvent(GenericEvent<ResourceState> eventRef)
    {
        event = eventRef;
    }

    public LinstorTriggerableEvent<ResourceState> get()
    {
        return event;
    }
}
