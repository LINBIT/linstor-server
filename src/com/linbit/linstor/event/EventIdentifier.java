package com.linbit.linstor.event;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;

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
}
