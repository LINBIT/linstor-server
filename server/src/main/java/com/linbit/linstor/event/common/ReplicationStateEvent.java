package com.linbit.linstor.event.common;

import com.linbit.linstor.event.GenericEvent;
import com.linbit.linstor.event.LinstorTriggerableEvent;
import com.linbit.linstor.layer.drbd.drbdstate.ReplState;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ReplicationStateEvent
{
    private final LinstorTriggerableEvent<ReplState> event;

    @Inject
    public ReplicationStateEvent(GenericEvent<ReplState> eventRef)
    {
        event = eventRef;
    }

    public LinstorTriggerableEvent<ReplState> get()
    {
        return event;
    }
}
