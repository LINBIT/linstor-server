package com.linbit.drbdmanage;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.stateflags.Flags;
import com.linbit.drbdmanage.stateflags.StateFlags;
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

    public StateFlags<VlmFlags> getFlags();

    public enum VlmFlags implements Flags
    {
        CLEAN(1L);

        public static final VlmFlags[] ALL_FLAGS =
        {
            CLEAN
        };

        public final long flagValue;

        private VlmFlags(long value)
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
