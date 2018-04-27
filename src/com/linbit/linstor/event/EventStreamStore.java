package com.linbit.linstor.event;

import com.linbit.linstor.LinStorDataAlreadyExistsException;

import java.util.Collection;

public interface EventStreamStore
{
    void addEventStream(EventIdentifier eventIdentifier)
        throws LinStorDataAlreadyExistsException;

    void removeEventStream(EventIdentifier eventIdentifier);

    Collection<EventIdentifier> getDescendantEventStreams(EventIdentifier eventIdentifier);
}
