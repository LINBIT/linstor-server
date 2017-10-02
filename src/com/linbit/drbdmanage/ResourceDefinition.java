package com.linbit.drbdmanage;

import com.linbit.TransactionObject;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;

import java.sql.SQLException;
import java.util.Iterator;
import com.linbit.drbdmanage.stateflags.Flags;
import com.linbit.drbdmanage.stateflags.StateFlags;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ResourceDefinition extends TransactionObject
{
    public UUID getUuid();

    public ObjectProtection getObjProt();

    public ResourceName getName();

    public VolumeDefinition getVolumeDfn(AccessContext accCtx, VolumeNumber volNr)
        throws AccessDeniedException;

    public Iterator<VolumeDefinition> iterateVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException;

    public Resource getResource(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException;

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    public StateFlags<RscDfnFlags> getFlags();

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public enum RscDfnFlags implements Flags
    {
        REMOVE(1L);

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

    public interface RscDfnApiData
    {
        String getName();
        Map<String, String> getProps();
        VolumeDefinition.VlmDfnApiData[] getVlmDfnList();
    }
}
