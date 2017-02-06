package com.linbit.drbdmanage;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import java.util.UUID;

/**
 *
 * @author raltnoeder
 */
public interface VolumeDefinition
{
    public UUID getUuid();

    public ResourceDefinition getResourceDfn();

    public VolumeNumber getVolumeNumber(AccessContext accCtx)
        throws AccessDeniedException;

    public MinorNumber getMinorNr(AccessContext accCtx)
        throws AccessDeniedException;

    public void setMinorNr(AccessContext accCtx, MinorNumber newMinorNr)
        throws AccessDeniedException;

    public long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException;

    public void setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException;
}
