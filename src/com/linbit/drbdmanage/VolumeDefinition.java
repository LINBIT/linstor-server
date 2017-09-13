package com.linbit.drbdmanage;

import com.linbit.TransactionObject;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.stateflags.Flags;
import com.linbit.drbdmanage.stateflags.StateFlags;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface VolumeDefinition extends TransactionObject
{
    public UUID getUuid();

    public ResourceDefinition getResourceDefinition();

    public VolumeNumber getVolumeNumber(AccessContext accCtx)
        throws AccessDeniedException;

    public MinorNumber getMinorNr(AccessContext accCtx)
        throws AccessDeniedException;

    public void setMinorNr(AccessContext accCtx, MinorNumber newMinorNr)
        throws AccessDeniedException, SQLException;

    public long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException;

    public void setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, SQLException;

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    public StateFlags<VlmDfnFlags> getFlags();

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public enum VlmDfnFlags implements Flags
    {
        REMOVE(1L);

        public final long flagValue;

        private VlmDfnFlags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }
    }

    public static interface CreationData
    {
        int getId();
        int getMinorNr();
        long getSize();
        Map<String, String> getProps();
    }
}
