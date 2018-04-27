package com.linbit.linstor.event;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;

import java.util.Objects;

public class EventIdentifier
{
    private final String eventName;

    private final NodeName nodeName;

    private final ResourceName resourceName;

    private final VolumeNumber volumeNumber;

    public EventIdentifier(
        String eventNameRef,
        NodeName nodeNameRef,
        ResourceName resourceNameRef,
        VolumeNumber volumeNumberRef
    )
    {
        if (volumeNumberRef != null && resourceNameRef == null)
        {
            throw new IllegalArgumentException("Event identifier with volume number but no resource name not allowed");
        }

        eventName = eventNameRef;
        nodeName = nodeNameRef;
        resourceName = resourceNameRef;
        volumeNumber = volumeNumberRef;
    }

    public String getEventName()
    {
        return eventName;
    }

    public NodeName getNodeName()
    {
        return nodeName;
    }

    public ResourceName getResourceName()
    {
        return resourceName;
    }

    public VolumeNumber getVolumeNumber()
    {
        return volumeNumber;
    }

    public ObjectIdentifier getObjectIdentifier()
    {
        return new ObjectIdentifier(nodeName, resourceName, volumeNumber);
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
            Objects.equals(nodeName, that.nodeName) &&
            Objects.equals(resourceName, that.resourceName) &&
            Objects.equals(volumeNumber, that.volumeNumber);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(eventName, nodeName, resourceName, volumeNumber);
    }

    @Override
    public String toString()
    {
        return eventName + "(" + getObjectIdentifier() + ")";
    }
}
