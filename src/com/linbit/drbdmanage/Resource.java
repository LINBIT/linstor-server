package com.linbit.drbdmanage;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Resource
{
    public Resource create(AccessContext accCtx, ResourceDefinition resDfnRef, Node nodeRef)
        throws AccessDeniedException;

    public UUID getUuid();

    public ResourceDefinition getDefinition();

    public Volume getVolume(VolumeNumber volNr);

    public Node getAssignedNode();
}
