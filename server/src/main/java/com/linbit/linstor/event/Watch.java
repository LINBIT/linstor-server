package com.linbit.linstor.event;

import java.util.Objects;
import java.util.UUID;

public class Watch
{
    private final UUID uuid;

    private final String peerId;

    private final Integer peerWatchId;

    private final EventIdentifier eventIdentifier;

    public Watch(
        UUID uuidRef,
        String peerIdRef,
        Integer peerWatchIdRef,
        EventIdentifier eventIdentifierRef
    )
    {
        uuid = uuidRef;
        peerId = peerIdRef;
        peerWatchId = peerWatchIdRef;
        eventIdentifier = eventIdentifierRef;
    }

    public UUID getUuid()
    {
        return uuid;
    }

    public String getPeerId()
    {
        return peerId;
    }

    public Integer getPeerWatchId()
    {
        return peerWatchId;
    }

    public EventIdentifier getEventIdentifier()
    {
        return eventIdentifier;
    }

    @Override
    // Single exit point exception: Automatically generated code
    @SuppressWarnings("DescendantToken")
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null || getClass() != obj.getClass())
        {
            return false;
        }
        Watch that = (Watch) obj;
        return Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(uuid);
    }
}
