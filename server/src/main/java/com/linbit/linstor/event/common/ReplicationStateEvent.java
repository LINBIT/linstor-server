package com.linbit.linstor.event.common;

import com.linbit.linstor.event.GenericEvent;
import com.linbit.linstor.event.LinstorTriggerableEvent;
import com.linbit.linstor.layer.drbd.drbdstate.ReplState;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ReplicationStateEvent
{
    private final LinstorTriggerableEvent<Pair<String, ReplState>> event;

    @Inject
    public ReplicationStateEvent(GenericEvent<Pair<String, ReplState>> eventRef)
    {
        event = eventRef;
    }

    public LinstorTriggerableEvent<Pair<String, ReplState>> get()
    {
        return event;
    }
}
