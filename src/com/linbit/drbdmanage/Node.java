package com.linbit.drbdmanage;

import com.linbit.drbdmanage.security.ObjectProtection;
import java.util.UUID;

/**
 *
 * @author raltnoeder
 */
public interface Node
{
    public UUID getUuid();

    public ObjectProtection getObjProt();

    public NodeName getName();

    public Resource getAssignedResource(ResourceName resName);
}
