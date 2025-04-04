package com.linbit.linstor.event.common;

import com.linbit.linstor.event.GenericEvent;
import com.linbit.linstor.event.LinstorTriggerableEvent;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Optional;

@Singleton
public class DonePercentageEvent
{
    private final LinstorTriggerableEvent<Pair<String, Optional<Float>>> event;

    @Inject
    public DonePercentageEvent(GenericEvent<Pair<String, Optional<Float>>> eventRef)
    {
        event = eventRef;
    }

    public LinstorTriggerableEvent<Pair<String, Optional<Float>>> get()
    {
        return event;
    }
}
