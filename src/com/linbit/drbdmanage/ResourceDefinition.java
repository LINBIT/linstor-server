package com.linbit.drbdmanage;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.stateflags.Flags;
import com.linbit.drbdmanage.stateflags.StateFlags;
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

    public Resource getResource(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException;

    public void addResource(AccessContext accCtx, Resource resRef)
        throws AccessDeniedException;

    public void removeResource(AccessContext accCtx, Resource resRef)
        throws AccessDeniedException;

    public ObjectProtection getObjProt();

    public StateFlags<RscDfnFlags> getFlags();

    public enum RscDfnFlags implements Flags
    {
        REMOVE(1L);

        public static final RscDfnFlags[] ALL_FLAGS =
        {
            REMOVE
        };

        public final long flagValue;

        private RscDfnFlags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }
    }
}
