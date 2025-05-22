package com.linbit.linstor.event.common;

import com.linbit.linstor.event.GenericEvent;
import com.linbit.linstor.event.LinstorTriggerableEvent;
import com.linbit.linstor.layer.drbd.drbdstate.ReplState;
import com.linbit.utils.PairNonNull;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ReplicationStateEvent
{
    private final LinstorTriggerableEvent<PairNonNull<String, ReplState>> event;

    @Inject
    public ReplicationStateEvent(GenericEvent<PairNonNull<String, ReplState>> eventRef)
    {
        event = eventRef;
    }

    public LinstorTriggerableEvent<PairNonNull<String, ReplState>> get()
    {
        return event;
    }
}
