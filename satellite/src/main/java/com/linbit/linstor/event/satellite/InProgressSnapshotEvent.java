package com.linbit.linstor.event.satellite;

import com.linbit.linstor.core.SnapshotState;
import com.linbit.linstor.event.GenericEvent;
import com.linbit.linstor.event.LinstorTriggerableEvent;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class InProgressSnapshotEvent
{
    private final LinstorTriggerableEvent<SnapshotState> event;

    @Inject
    public InProgressSnapshotEvent(GenericEvent<SnapshotState> eventRef)
    {
        event = eventRef;
    }

    public LinstorTriggerableEvent<SnapshotState> get()
    {
        return event;
    }
}
