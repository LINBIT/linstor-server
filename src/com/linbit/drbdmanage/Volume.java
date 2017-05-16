package com.linbit.drbdmanage;

import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Volume
{
    public UUID getUuid();

    public Resource getResource();

    public ResourceDefinition getResourceDfn();

    public VolumeDefinition getVolumeDfn();

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;
}
