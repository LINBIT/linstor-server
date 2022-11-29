package com.linbit.linstor.core.objects;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;

import java.util.Objects;

public class ResourceConnectionKey implements Comparable<ResourceConnectionKey>
{
    private final Resource source;
    private final Resource target;

    private final NodeName srcNodeName;
    private final NodeName tgtNodeName;

    public ResourceConnectionKey(final Resource resourceA, final Resource resourceB)
    {
        final NodeName rscNameA = resourceA.getNode().getName();
        final NodeName rscNameB = resourceB.getNode().getName();

        if (rscNameA.compareTo(rscNameB) < 0)
        {
            source = resourceA;
            target = resourceB;
            srcNodeName = rscNameA;
            tgtNodeName = rscNameB;
        }
        else
        {
            source = resourceB;
            target = resourceA;
            srcNodeName = rscNameB;
            tgtNodeName = rscNameA;
        }
    }

    public Resource getSource()
    {
        return source;
    }

    public Resource getTarget()
    {
        return target;
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
                ResourceName rsc1SrcName = source.getResourceDefinition().getName();
                ResourceName rsc2SrcName = other.getSource().getResourceDefinition().getName();
                result = rsc1SrcName.compareTo(rsc2SrcName);
            }
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, srcNodeName, target, tgtNodeName);
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
            ret = Objects.equals(source, other.source) && Objects.equals(srcNodeName, other.srcNodeName) &&
                Objects.equals(target, other.target) && Objects.equals(tgtNodeName, other.tgtNodeName);
        }
        return ret;
    }

}
