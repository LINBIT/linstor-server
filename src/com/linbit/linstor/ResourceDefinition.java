package com.linbit.linstor;

import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ResourceDefinition extends TransactionObject, DbgInstanceUuid, Comparable<ResourceDefinition>
{
    UUID getUuid();

    ObjectProtection getObjProt();

    ResourceName getName();

    TcpPortNumber getPort(AccessContext accCtx) throws AccessDeniedException;

    TcpPortNumber setPort(AccessContext accCtx, TcpPortNumber port)
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ValueInUseException;

    int getVolumeDfnCount(AccessContext accCtx)
        throws AccessDeniedException;

    VolumeDefinition getVolumeDfn(AccessContext accCtx, VolumeNumber volNr)
        throws AccessDeniedException;

    Iterator<VolumeDefinition> iterateVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException;

    Stream<VolumeDefinition> streamVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException;

    int getResourceCount();

    Iterator<Resource> iterateResource(AccessContext accCtx)
        throws AccessDeniedException;

    Stream<Resource> streamResource(AccessContext accCtx)
        throws AccessDeniedException;

    void copyResourceMap(
        AccessContext accCtx, Map<? super NodeName, ? super Resource> dstMap
    )
        throws AccessDeniedException;

    Resource getResource(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException;

    void addResource(AccessContext accCtx, Resource resRef)
        throws AccessDeniedException;

    String getSecret(AccessContext accCtx)
        throws AccessDeniedException;

    TransportType getTransportType(AccessContext accCtx)
        throws AccessDeniedException;

    TransportType setTransportType(AccessContext accCtx, TransportType type)
        throws AccessDeniedException, SQLException;

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    StateFlags<RscDfnFlags> getFlags();

    void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    RscDfnApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    @Override
    default int compareTo(ResourceDefinition otherRscDfn)
    {
        return getName().compareTo(otherRscDfn.getName());
    }

    enum RscDfnFlags implements Flags
    {
        DELETE(1L);

        public final long flagValue;

        RscDfnFlags(long value)
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

    enum TransportType
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
                    throw new IllegalArgumentException(
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

    public interface InitMaps
    {
        Map<NodeName, Resource> getRscMap();
        Map<VolumeNumber, VolumeDefinition> getVlmDfnMap();
    }
}
