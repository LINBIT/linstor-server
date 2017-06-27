package com.linbit.drbdmanage;

import java.util.UUID;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ConnectionDefinition
{
    public UUID getUuid();

    public ResourceDefinition getResourceDefinition(AccessContext accCtx) throws AccessDeniedException;

    public int getPort(AccessContext accCtx) throws AccessDeniedException;
}
