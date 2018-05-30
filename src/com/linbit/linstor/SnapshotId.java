package com.linbit.linstor;

import java.util.Objects;

/**
 * Identifies a snapshot within a node.
 */
public class SnapshotId implements Comparable<SnapshotId>
{
    private final ResourceName resourceName;

    private final SnapshotName snapshotName;

    public SnapshotId(ResourceName resourceNameRef, SnapshotName snapshotNameRef)
    {
        resourceName = resourceNameRef;
        snapshotName = snapshotNameRef;
    }

    public ResourceName getResourceName()
    {
        return resourceName;
    }

    public SnapshotName getSnapshotName()
    {
        return snapshotName;
    }

    @Override
    // Code style exception: Automatically generated code
    @SuppressWarnings({"DescendantToken", "ParameterName"})
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        SnapshotId that = (SnapshotId) o;
        return Objects.equals(resourceName, that.resourceName) &&
            Objects.equals(snapshotName, that.snapshotName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(resourceName, snapshotName);
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(SnapshotId other)
    {
        int eq = resourceName.compareTo(other.resourceName);
        if (eq == 0)
        {
            eq = snapshotName.compareTo(other.snapshotName);
        }
        return eq;
    }
}
