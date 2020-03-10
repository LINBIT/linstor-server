package com.linbit.linstor.event;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;

import java.util.Objects;

public class ObjectIdentifier
{
    private final NodeName nodeName;
    private final NodeName peerNodeName;

    private final ResourceName resourceName;

    private final VolumeNumber volumeNumber;

    private final SnapshotName snapshotName;

    public static ObjectIdentifier global()
    {
        return new ObjectIdentifier(null, null, null, null, null);
    }

    public static ObjectIdentifier node(NodeName nodeName)
    {
        return new ObjectIdentifier(nodeName, null, null, null, null);
    }

    /**
     * When used on a satellite, the node name is implicit, so this represents a resource.
     */
    public static ObjectIdentifier resourceDefinition(ResourceName resourceName)
    {
        return new ObjectIdentifier(null, resourceName, null, null, null);
    }

    /**
     * When used on a satellite, the node name is implicit, so this represents a volume.
     */
    public static ObjectIdentifier volumeDefinition(
        ResourceName resourceName, VolumeNumber volumeNumber)
    {
        return new ObjectIdentifier(null, resourceName, volumeNumber, null, null);
    }

    public static ObjectIdentifier resource(NodeName nodeName, ResourceName resourceName)
    {
        return new ObjectIdentifier(nodeName, resourceName, null, null, null);
    }

    public static ObjectIdentifier volume(
        NodeName nodeName, ResourceName resourceName, VolumeNumber volumeNumber)
    {
        return new ObjectIdentifier(nodeName, resourceName, volumeNumber, null, null);
    }

    /**
     * When used on a satellite, the node name is implicit, so this represents a snapshot.
     */
    public static ObjectIdentifier snapshotDefinition(
        ResourceName resourceName, SnapshotName snapshotName)
    {
        return new ObjectIdentifier(null, resourceName, null, snapshotName, null);
    }

    public static ObjectIdentifier snapshot(
        NodeName nodeName, ResourceName resourceName, SnapshotName snapshotName)
    {
        return new ObjectIdentifier(nodeName, resourceName, null, snapshotName, null);
    }

    public static ObjectIdentifier connection(
        NodeName localNode, NodeName remoteNode, ResourceName resourceName
    )
    {
        return new ObjectIdentifier(localNode, resourceName, null, null, remoteNode);
    }

    public ObjectIdentifier(
        NodeName nodeNameRef,
        ResourceName resourceNameRef,
        VolumeNumber volumeNumberRef,
        SnapshotName snapshotNameRef,
        NodeName remoteNodeRef
    )
    {
        if (volumeNumberRef != null && resourceNameRef == null)
        {
            throw new IllegalArgumentException("Object identifier with volume number but no resource name not allowed");
        }
        if (snapshotNameRef != null && resourceNameRef == null)
        {
            throw new IllegalArgumentException("Object identifier with snapshot name but no resource name not allowed");
        }

        nodeName = nodeNameRef;
        resourceName = resourceNameRef;
        volumeNumber = volumeNumberRef;
        snapshotName = snapshotNameRef;
        peerNodeName = remoteNodeRef;
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

    public SnapshotName getSnapshotName()
    {
        return snapshotName;
    }

    public NodeName getPeerNodeName()
    {
        return peerNodeName;
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
            Objects.equals(volumeNumber, that.volumeNumber) &&
            Objects.equals(snapshotName, that.snapshotName) &&
            Objects.equals(peerNodeName, that.peerNodeName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeName, resourceName, volumeNumber, snapshotName, peerNodeName);
    }

    @Override
    public String toString()
    {
        return (nodeName == null ? "" : nodeName) +
            "/" + (resourceName == null ? "" : resourceName) +
            (volumeNumber == null ? "" : "/" + volumeNumber) +
            (snapshotName == null ? "" : "@" + snapshotName);
    }
}
