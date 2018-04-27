package com.linbit.linstor.event;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;

import java.util.Objects;

public class ObjectIdentifier
{
    private final NodeName nodeName;

    private final ResourceName resourceName;

    private final VolumeNumber volumeNumber;

    public ObjectIdentifier(
        NodeName nodeNameRef,
        ResourceName resourceNameRef,
        VolumeNumber volumeNumberRef
    )
    {
        if (volumeNumberRef != null && resourceNameRef == null)
        {
            throw new IllegalArgumentException("Object identifier with volume number but no resource name not allowed");
        }

        nodeName = nodeNameRef;
        resourceName = resourceNameRef;
        volumeNumber = volumeNumberRef;
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
        ObjectIdentifier that = (ObjectIdentifier) obj;
        return Objects.equals(nodeName, that.nodeName) &&
            Objects.equals(resourceName, that.resourceName) &&
            Objects.equals(volumeNumber, that.volumeNumber);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeName, resourceName, volumeNumber);
    }

    @Override
    public String toString()
    {
        return (nodeName == null ? "" : nodeName) +
            "/" + (resourceName == null ? "" : resourceName) +
            (volumeNumber == null ? "" : "/" + volumeNumber);
    }
}
