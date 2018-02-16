package com.linbit.linstor;

import com.linbit.TransactionObject;
import com.linbit.linstor.netcom.Peer;
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
public interface Node extends TransactionObject, DbgInstanceUuid, Comparable<Node>
{
    UUID getUuid();

    ObjectProtection getObjProt();

    NodeName getName();

    NetInterface getNetInterface(AccessContext accCtx, NetInterfaceName niName)
        throws AccessDeniedException;

    Iterator<NetInterface> iterateNetInterfaces(AccessContext accCtx)
        throws AccessDeniedException;

    Resource getResource(AccessContext accCtx, ResourceName resName)
        throws AccessDeniedException;

    void addResource(AccessContext accCtx, Resource resRef)
        throws AccessDeniedException;

    NodeConnection getNodeConnection(AccessContext accCtx, Node otherNode)
        throws AccessDeniedException;

    void setNodeConnection(AccessContext accCtx, NodeConnection nodeConnection)
        throws AccessDeniedException;

    void removeNodeConnection(AccessContext accCtx, NodeConnection nodeConnection)
        throws AccessDeniedException;

    int getResourceCount();

    Iterator<Resource> iterateResources(AccessContext accCtx)
        throws AccessDeniedException;

    void copyStorPoolMap(AccessContext accCtx, Map<? super StorPoolName, ? super StorPool> dstMap)
        throws AccessDeniedException;

    int getStorPoolCount();

    StorPool getStorPool(AccessContext accCtx, StorPoolName poolName)
        throws AccessDeniedException;

    StorPool getDisklessStorPool(AccessContext accCtx)
        throws AccessDeniedException;

    Iterator<StorPool> iterateStorPools(AccessContext accCtx)
        throws AccessDeniedException;

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    NodeType getNodeType(AccessContext accCtx)
        throws AccessDeniedException;

    boolean hasNodeType(AccessContext accCtx, NodeType reqType)
        throws AccessDeniedException;

    StateFlags<NodeFlag> getFlags();

    void setPeer(AccessContext accCtx, Peer peer) throws AccessDeniedException;

    Peer getPeer(AccessContext accCtx) throws AccessDeniedException;

    SatelliteConnection getSatelliteConnection(AccessContext accCtx)
        throws AccessDeniedException;

    void setSatelliteConnection(AccessContext accCtx, SatelliteConnection stltConn)
        throws AccessDeniedException, SQLException;

    void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    NodeApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException;

    enum NodeType implements Flags
    {
        CONTROLLER(1),
        SATELLITE(2),
        COMBINED(3),

        AUXILIARY(4);

        private final int flag;

        NodeType(int flagValue)
        {
            flag = flagValue;
        }

        @Override
        public long getFlagValue()
        {
            return flag;
        }

        public static NodeType getByValue(long value)
        {
            NodeType ret = null;
            for (NodeType type : NodeType.values())
            {
                if (type.flag == value)
                {
                    ret = type;
                    break;
                }
            }
            return ret;
        }

        public static NodeType valueOfIgnoreCase(String string, NodeType defaultValue)
            throws IllegalArgumentException
        {
            NodeType ret = defaultValue;
            if (string != null)
            {
                NodeType val = valueOf(string.toUpperCase());
                if (val != null)
                {
                    ret = val;
                }
            }
            return ret;
        }
    }

    enum NodeFlag implements Flags
    {
        DELETE(1L),
        QIGNORE(0x10000L);

        public final long flagValue;

        NodeFlag(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static NodeFlag[] valuesOfIgnoreCase(String string)
        {
            NodeFlag[] flags;
            if (string == null)
            {
                flags = new NodeFlag[0];
            }
            else
            {
                String[] split = string.split(",");
                flags = new NodeFlag[split.length];

                for (int idx = 0; idx < split.length; idx++)
                {
                    flags[idx] = NodeFlag.valueOf(split[idx].toUpperCase().trim());
                }
            }
            return flags;
        }

        public static NodeFlag[] restoreFlags(long nodeFlags)
        {
            List<NodeFlag> flagList = new ArrayList<>();
            for (NodeFlag flag : NodeFlag.values())
            {
                if ((nodeFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new NodeFlag[flagList.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(NodeFlag.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(NodeFlag.class, listFlags);
        }
    }

    public interface NodeApi
    {
        String getName();
        String getType();
        UUID getUuid();
        Boolean isConnected();
        Map<String, String> getProps();
        long getFlags();
        List<NetInterface.NetInterfaceApi> getNetInterfaces();
        UUID getDisklessStorPoolUuid();
    }
}
