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
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Node extends TransactionObject
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

    public Iterator<Resource> iterateResources(AccessContext accCtx)
        throws AccessDeniedException;

    public StorPool getStorPool(AccessContext accCtx, StorPoolName poolName)
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

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

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

        public static NodeType valueOfIgnoreCase(String string)
        {
            return valueOf(string.toUpperCase());
        }
    }

    public enum NodeFlag implements Flags
    {
        REMOVE(1L),
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
            String[] split = string.split(",");
            NodeFlag[] flags = new NodeFlag[split.length];

            for (int i = 0; i < split.length; i++)
            {
                flags[i] = NodeFlag.valueOf(split[i].toUpperCase().trim());
            }

            return flags;
        }
    }
}
