package com.linbit.linstor.event.writer;

import com.linbit.linstor.event.ObjectIdentifier;

public interface EventWriter
{
    /**
     * @return null if no event should be sent.
     */
    byte[] writeEvent(ObjectIdentifier objectIdentifier) throws Exception;

    default void clear(ObjectIdentifier objectIdentifier) throws Exception
    {
        // Nothing to clear by default
    }
}
