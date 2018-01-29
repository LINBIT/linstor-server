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
public interface Node extends TransactionObject, DbgInstanceUuid
{
    public UUID getUuid();

    public ObjectProtection getObjProt();

    public NodeName getName();

    public NetInterface getNetInterface(AccessContext accCtx, NetInterfaceName niName)
        throws AccessDeniedException;

    public Iterator<NetInterface> iterateNetInterfaces(AccessContext accCtx)
        throws AccessDeniedException;

    public Resource getResource(AccessContext accCtx, ResourceName resName)
        throws AccessDeniedException;

    public void addResource(AccessContext accCtx, Resource resRef)
        throws AccessDeniedException;

    public NodeConnection getNodeConnection(AccessContext accCtx, Node otherNode)
        throws AccessDeniedException;

    public void setNodeConnection(AccessContext accCtx, NodeConnection nodeConnection)
        throws AccessDeniedException;

    public void removeNodeConnection(AccessContext accCtx, NodeConnection nodeConnection)
        throws AccessDeniedException;

    public int getResourceCount();

    public Iterator<Resource> iterateResources(AccessContext accCtx)
        throws AccessDeniedException;

    public int getStorPoolCount();

    public StorPool getStorPool(AccessContext accCtx, StorPoolName poolName)
        throws AccessDeniedException;

    public StorPool getDisklessStorPool(AccessContext accCtx)
        throws AccessDeniedException;

    public Iterator<StorPool> iterateStorPools(AccessContext accCtx)
        throws AccessDeniedException;

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    public NodeType getNodeType(AccessContext accCtx)
        throws AccessDeniedException;

    public boolean hasNodeType(AccessContext accCtx, NodeType reqType)
        throws AccessDeniedException;

    public StateFlags<NodeFlag> getFlags();

    public void setPeer(AccessContext accCtx, Peer peer) throws AccessDeniedException;

    public Peer getPeer(AccessContext accCtx) throws AccessDeniedException;

    public SatelliteConnection getSatelliteConnection(AccessContext accCtx)
        throws AccessDeniedException;

    public void setSatelliteConnection(AccessContext accCtx, SatelliteConnection stltConn)
        throws AccessDeniedException;

    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public NodeApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException;

    public enum NodeType implements Flags
    {
        CONTROLLER(1),
        SATELLITE(2),
        COMBINED(3),

        AUXILIARY(4);

        private final int flag;

        private NodeType(int flag)
        {
            this.flag = flag;
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

    public enum NodeFlag implements Flags
    {
        DELETE(1L),
        QIGNORE(0x10000L);

        public final long flagValue;

        private NodeFlag(long value)
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

                for (int i = 0; i < split.length; i++)
                {
                    flags[i] = NodeFlag.valueOf(split[i].toUpperCase().trim());
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
