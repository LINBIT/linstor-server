package com.linbit.drbdmanage;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Node
{
    public UUID getUuid();

    public ObjectProtection getObjProt();

    public NodeName getName();

    public NetInterface getNetInterface(AccessContext accCtx, NetInterfaceName niName)
        throws AccessDeniedException;

    public void addNetInterface(AccessContext accCtx, NetInterface niRef)
        throws AccessDeniedException;

    public void removeNetInterface(AccessContext accCtx, NetInterface niRef)
        throws AccessDeniedException;

    public Resource getResource(AccessContext accCtx, ResourceName resName)
        throws AccessDeniedException;

    public void addResource(AccessContext accCtx, Resource resRef)
        throws AccessDeniedException;

    public void removeResource(AccessContext accCtx, Resource resRef)
        throws AccessDeniedException;

    public StorPool getStorPool(AccessContext accCtx, StorPoolName poolName)
        throws AccessDeniedException;

    public void addStorPool(AccessContext accCtx, StorPool pool)
        throws AccessDeniedException;

    public void removeStorPool(AccessContext accCtx, StorPool pool)
        throws AccessDeniedException;
}
