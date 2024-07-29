package com.linbit.linstor.event;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;

import java.util.Objects;

public class EventIdentifier
{
    private final @Nullable String eventName;

    private final ObjectIdentifier objectIdentifier;

    public static EventIdentifier global(@Nullable String eventName)
    {
        return new EventIdentifier(eventName, ObjectIdentifier.global());
    }

    public static EventIdentifier node(String eventName, NodeName nodeName)
    {
        return new EventIdentifier(eventName, ObjectIdentifier.node(nodeName));
    }

    /**
     * When used on a satellite, the node name is implicit, so this represents a resource.
     */
    public static EventIdentifier resourceDefinition(String eventName, ResourceName resourceName)
    {
        return new EventIdentifier(eventName, ObjectIdentifier.resourceDefinition(resourceName));
    }

    /**
     * When used on a satellite, the node name is implicit, so this represents a volume.
     */
    public static EventIdentifier volumeDefinition(
        String eventName, ResourceName resourceName, VolumeNumber volumeNumber)
    {
        return new EventIdentifier(eventName, ObjectIdentifier.volumeDefinition(resourceName, volumeNumber));
    }

    public static EventIdentifier resource(String eventName, NodeName nodeName, ResourceName resourceName)
    {
        return new EventIdentifier(eventName, ObjectIdentifier.resource(nodeName, resourceName));
    }

    public static EventIdentifier volume(
        String eventName, NodeName nodeName, ResourceName resourceName, VolumeNumber volumeNumber)
    {
        return new EventIdentifier(eventName, ObjectIdentifier.volume(nodeName, resourceName, volumeNumber));
    }

    /**
     * When used on a satellite, the node name is implicit, so this represents a snapshot.
     */
    public static EventIdentifier snapshotDefinition(
        String eventName, ResourceName resourceName, SnapshotName snapshotName)
    {
        return new EventIdentifier(eventName, ObjectIdentifier.snapshotDefinition(resourceName, snapshotName));
    }

    public static EventIdentifier snapshot(
        @Nullable String eventName,
        NodeName nodeName,
        ResourceName resourceName,
        SnapshotName snapshotName
    )
    {
        return new EventIdentifier(eventName, ObjectIdentifier.snapshot(nodeName, resourceName, snapshotName));
    }

    public EventIdentifier(
        @Nullable String eventNameRef,
        ObjectIdentifier objectIdentifierRef
    )
    {
        eventName = eventNameRef;
        objectIdentifier = objectIdentifierRef;
    }

    public @Nullable String getEventName()
    {
        return eventName;
    }

    public @Nullable NodeName getNodeName()
    {
        return objectIdentifier.getNodeName();
    }

    public @Nullable ResourceName getResourceName()
    {
        return objectIdentifier.getResourceName();
    }

    public @Nullable VolumeNumber getVolumeNumber()
    {
        return objectIdentifier.getVolumeNumber();
    }

    public @Nullable SnapshotName getSnapshotName()
    {
        return objectIdentifier.getSnapshotName();
    }

    public @Nullable NodeName getPeerNodeName()
    {
        return objectIdentifier.getPeerNodeName();
    }

    public ObjectIdentifier getObjectIdentifier()
    {
        return objectIdentifier;
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
        EventIdentifier that = (EventIdentifier) obj;
        return Objects.equals(eventName, that.eventName) &&
            Objects.equals(objectIdentifier, that.objectIdentifier);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(eventName, objectIdentifier);
    }

    @Override
    public String toString()
    {
        return eventName + "(" + getObjectIdentifier() + ")";
    }
}
