package com.linbit.drbdmanage;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import java.util.UUID;

/**
 *
 * @author raltnoeder
 */
public interface ResourceDefinition
{
    public UUID getUuid();

    public ResourceName getName();

    public ConnectionDefinition getConnectionDfn(AccessContext accCtx, NodeName clNodeName, Integer connNr)
        throws AccessDeniedException;

    public VolumeDefinition getVolumeDfn(AccessContext accCtx, VolumeNumber volNr)
        throws AccessDeniedException;
}
