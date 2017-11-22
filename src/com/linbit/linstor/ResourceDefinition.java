package com.linbit.linstor;

import com.linbit.TransactionObject;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.StateFlags;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

    public TcpPortNumber getPort(AccessContext accCtx) throws AccessDeniedException;

    public void setPort(AccessContext accCtx, TcpPortNumber port) throws AccessDeniedException, SQLException;

    public VolumeDefinition getVolumeDfn(AccessContext accCtx, VolumeNumber volNr)
        throws AccessDeniedException;

    public Iterator<VolumeDefinition> iterateVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException;

    public int getResourceCount();

    public Iterator<Resource> iterateResource(AccessContext accCtx)
        throws AccessDeniedException;

    public Resource getResource(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException;

    public String getSecret(AccessContext accCtx)
        throws AccessDeniedException;

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    public StateFlags<RscDfnFlags> getFlags();

    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public enum RscDfnFlags implements Flags
    {
        DELETE(1L);

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

        public static RscDfnFlags[] restoreFlags(long rawFlags)
        {
            List<RscDfnFlags> list = new ArrayList<>();
            for (RscDfnFlags flag : RscDfnFlags.values())
            {
                if ((rawFlags & flag.flagValue) == flag.flagValue)
                {
                    list.add(flag);
                }
            }
            return list.toArray(new RscDfnFlags[list.size()]);
        }
    }

    public interface RscDfnApiData
    {
        String getName();
        Map<String, String> getProps();
        VolumeDefinition.VlmDfnApi[] getVlmDfnList();
    }
}
