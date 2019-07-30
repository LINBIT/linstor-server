package com.linbit.linstor.core.objects;

import com.linbit.linstor.core.identifier.NodeName;

import java.util.Comparator;

public class ResourceConnectionKey
{
    public static final Comparator<ResourceConnectionKey> COMPARATOR =
        (key1st, key2nd) ->
        {
            int result = key1st.srcNodeName.compareTo(key2nd.srcNodeName);
            if (result == 0)
            {
                result = key1st.tgtNodeName.compareTo(key2nd.tgtNodeName);
            }
            return result;
        };

    private final Resource source;
    private final Resource target;

    private final NodeName srcNodeName;
    private final NodeName tgtNodeName;

    public ResourceConnectionKey(final Resource resourceA, final Resource resourceB)
    {
        final NodeName rscNameA = resourceA.getAssignedNode().getName();
        final NodeName rscNameB = resourceB.getAssignedNode().getName();

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
}
