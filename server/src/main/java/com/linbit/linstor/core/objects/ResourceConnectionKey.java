package com.linbit.linstor.core.objects;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;

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
                if (result == 0)
                {
                    ResourceName rsc1SrcName = key1st.getSource().getResourceDefinition().getName();
                    ResourceName rsc2SrcName = key2nd.getSource().getResourceDefinition().getName();
                    result = rsc1SrcName.compareTo(rsc2SrcName);
                }
            }
            return result;
        };

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
}
