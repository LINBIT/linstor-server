package com.linbit.drbdmanage;

import java.util.UUID;

/**
 *
 * @author raltnoeder
 */
public interface Node
{
    public UUID getUuid();

    public NodeName getName();

    public Resource getAssignedResource(ResourceName resName);
}
