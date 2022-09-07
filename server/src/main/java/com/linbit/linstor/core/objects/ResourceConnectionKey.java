package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;

import java.util.Objects;

public class ResourceConnectionKey implements Comparable<ResourceConnectionKey>
{
    private final ResourceName rscName;

    private final NodeName srcNodeName;
    private final NodeName tgtNodeName;

    public ResourceConnectionKey(final Resource resourceA, final Resource resourceB)
    {
        final NodeName rscNameA = resourceA.getNode().getName();
        final NodeName rscNameB = resourceB.getNode().getName();
        if (!resourceA.getResourceDefinition().equals(resourceB.getResourceDefinition()))
        {
            throw new ImplementationError(
                String.format(
                    "Creating connection between unrelated Resources %n" +
                        "Volume1: NodeName=%s, ResName=%s %n" +
                        "Volume2: NodeName=%s, ResName=%s.",
                    rscNameA.value,
                    resourceA.getResourceDefinition().getName().value,
                    rscNameB.value,
                    resourceB.getResourceDefinition().getName().value
                ),
                null
            );
        }

        rscName = resourceA.getResourceDefinition().getName();

        if (rscNameA.compareTo(rscNameB) < 0)
        {
            srcNodeName = rscNameA;
            tgtNodeName = rscNameB;
        }
        else
        {
            srcNodeName = rscNameB;
            tgtNodeName = rscNameA;
        }
    }

    public ResourceName getResourceName()
    {
        return rscName;
    }

    public NodeName getSourceNodeName()
    {
        return srcNodeName;
    }

    public NodeName getTargetNodeName()
    {
        return tgtNodeName;
    }

    @Override
    public int compareTo(ResourceConnectionKey other)
    {
        int result = srcNodeName.compareTo(other.srcNodeName);
        if (result == 0)
        {
            result = tgtNodeName.compareTo(other.tgtNodeName);
            if (result == 0)
            {
                result = rscName.compareTo(other.rscName);
            }
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rscName, srcNodeName, tgtNodeName);
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof ResourceConnectionKey)
        {
            ResourceConnectionKey other = (ResourceConnectionKey) obj;
            ret = Objects.equals(rscName, other.rscName) && Objects.equals(srcNodeName, other.srcNodeName) &&
                Objects.equals(tgtNodeName, other.tgtNodeName);
        }
        return ret;
    }

}
