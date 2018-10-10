package com.linbit.linstor;

public class ResourceConnectionKey
{
    private final Resource source;
    private final Resource target;

    public ResourceConnectionKey(final Resource resourceA, final Resource resourceB)
    {
        if (resourceA.getAssignedNode().getName().compareTo(resourceB.getAssignedNode().getName()) < 0)
        {
            source = resourceA;
            target = resourceB;
        }
        else
        {
            source = resourceB;
            target = resourceA;
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
}
