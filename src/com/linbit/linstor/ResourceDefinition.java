package com.linbit.linstor;

import com.linbit.TransactionObject;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
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

    public void copyResourceMap(
        AccessContext accCtx, Map<? super NodeName, ? super Resource> dstMap
    )
        throws AccessDeniedException;

    public Resource getResource(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException;

    public String getSecret(AccessContext accCtx)
        throws AccessDeniedException;

    public TransportType getTransportType(AccessContext accCtx)
        throws AccessDeniedException;

    public void setTransportType(AccessContext accCtx, TransportType type)
        throws AccessDeniedException, SQLException;

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    public StateFlags<RscDfnFlags> getFlags();

    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public RscDfnApi getApiData(AccessContext accCtx) throws AccessDeniedException;

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

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(RscDfnFlags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(RscDfnFlags.class, listFlags);
        }
    }

    public static enum TransportType
    {
        IP, RDMA, RoCE;

        public static TransportType byValue(String str)
        {
            TransportType type = null;
            switch (str.toUpperCase())
            {
                case "IP":
                    type = IP;
                    break;
                case "RDMA":
                    type = RDMA;
                    break;
                case "ROCE":
                    type = RoCE;
                    break;
                default:
                    throw new LinStorRuntimeException(
                        "Unknown TransportType: '" + str + "'"
                    );
            }
            return type;
        }

        public static TransportType valueOfIgnoreCase(String string, TransportType defaultValue)
            throws IllegalArgumentException
        {
            TransportType ret = defaultValue;
            if (string != null)
            {
                TransportType val = valueOf(string.toUpperCase());
                if (val != null)
                {
                    ret = val;
                }
            }
            return ret;
        }
    }

    public interface RscDfnApi
    {
        UUID getUuid();
        String getResourceName();
        int getPort();
        String getSecret();
        long getFlags();
        Map<String, String> getProps();
        List<VolumeDefinition.VlmDfnApi> getVlmDfnList();
        String getTransportType();
    }
}
