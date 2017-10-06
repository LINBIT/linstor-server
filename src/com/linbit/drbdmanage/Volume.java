package com.linbit.drbdmanage;

import com.linbit.TransactionObject;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.stateflags.Flags;
import com.linbit.drbdmanage.stateflags.StateFlags;

import java.sql.SQLException;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Volume extends TransactionObject
{
    public UUID getUuid();

    public Resource getResource();

    public ResourceDefinition getResourceDefinition();

    public VolumeDefinition getVolumeDefinition();

    public Props getProps(AccessContext accCtx) throws AccessDeniedException;

    public StateFlags<VlmFlags> getFlags();

    public VolumeConnection getVolumeConnection(AccessContext dbCtx, Volume otherVol)
        throws AccessDeniedException;

    void setVolumeConnection(AccessContext accCtx, VolumeConnectionData volumeConnection)
        throws AccessDeniedException;

    void removeVolumeConnection(AccessContext accCtx, VolumeConnectionData volumeConnection)
        throws AccessDeniedException;

    public String getBlockDevicePath(AccessContext accCtx) throws AccessDeniedException;

    public String getMetaDiskPath(AccessContext accCtx) throws AccessDeniedException;

    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException;

    public enum VlmFlags implements Flags
    {
        CLEAN(1L);

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

    public interface VlmApi
    {
        public String getBlockDevice();
        public String getMetaDisk();
        public int getVlmNr();
    }
}
