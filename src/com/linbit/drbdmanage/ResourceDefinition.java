package com.linbit.drbdmanage;

import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.util.Iterator;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ResourceDefinition
{
    public UUID getUuid();

    public ObjectProtection getObjProt();

    public ResourceName getName();

    public ConnectionDefinition getConnectionDfn(AccessContext accCtx, NodeName clNodeName, Integer connNr)
        throws AccessDeniedException;

    public VolumeDefinition getVolumeDfn(AccessContext accCtx, VolumeNumber volNr)
        throws AccessDeniedException;

    public Iterator<VolumeDefinition> iterateVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException;

    public Resource getResource(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException;

    public void addResource(AccessContext accCtx, Resource resRef)
        throws AccessDeniedException;

    public void removeResource(AccessContext accCtx, Resource resRef)
        throws AccessDeniedException;

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;
}
