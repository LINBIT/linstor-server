package com.linbit.linstor.event.common;

import com.linbit.linstor.event.GenericEvent;
import com.linbit.linstor.event.LinstorTriggerableEvent;
import com.linbit.utils.PairNonNull;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Optional;

@Singleton
public class DonePercentageEvent
{
    private final LinstorTriggerableEvent<PairNonNull<String, Optional<Float>>> event;

    @Inject
    public DonePercentageEvent(GenericEvent<PairNonNull<String, Optional<Float>>> eventRef)
    {
        event = eventRef;
    }

    public LinstorTriggerableEvent<PairNonNull<String, Optional<Float>>> get()
    {
        return event;
    }
}
