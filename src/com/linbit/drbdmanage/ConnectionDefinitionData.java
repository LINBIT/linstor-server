package com.linbit.drbdmanage;

import java.util.UUID;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;

/**
 * Defines a connection for a DRBD resource
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ConnectionDefinitionData implements ConnectionDefinition
{
    // Object identifier
    private UUID objId;

    private ObjectProtection objProt;

    private int port;

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public ResourceDefinition getResourceDefinition(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return null;
    }

    @Override
    public int getPort(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return port;
    }
}
