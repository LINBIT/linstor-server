package com.linbit.drbdmanage;

import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.util.Iterator;
import com.linbit.drbdmanage.stateflags.Flags;
import com.linbit.drbdmanage.stateflags.StateFlags;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Node
{
    public UUID getUuid();

    public ObjectProtection getObjProt();

    public NodeName getName();

    public NetInterface getNetInterface(AccessContext accCtx, NetInterfaceName niName)
        throws AccessDeniedException;

    public void addNetInterface(AccessContext accCtx, NetInterface niRef)
        throws AccessDeniedException;

    public void removeNetInterface(AccessContext accCtx, NetInterface niRef)
        throws AccessDeniedException;

    public Iterator<NetInterface> iterateNetInterfaces(AccessContext accCtx)
        throws AccessDeniedException;

    public Resource getResource(AccessContext accCtx, ResourceName resName)
        throws AccessDeniedException;

    public void addResource(AccessContext accCtx, Resource resRef)
        throws AccessDeniedException;

    public void removeResource(AccessContext accCtx, Resource resRef)
        throws AccessDeniedException;

    public Iterator<Resource> iterateResources(AccessContext accCtx)
        throws AccessDeniedException;

    public StorPool getStorPool(AccessContext accCtx, StorPoolName poolName)
        throws AccessDeniedException;

    public void addStorPool(AccessContext accCtx, StorPool pool)
        throws AccessDeniedException;

    public void removeStorPool(AccessContext accCtx, StorPool pool)
        throws AccessDeniedException;

    public Iterator<StorPool> iterateStorPools(AccessContext accCtx)
        throws AccessDeniedException;

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    public Iterator<NodeType> iterateNodeTypes(AccessContext accCtx)
        throws AccessDeniedException;

    public boolean hasNodeType(AccessContext accCtx, NodeType reqType)
        throws AccessDeniedException;

    public StateFlags<NodeFlags> getFlags();

    public enum NodeType
    {
        CONTROLLER,
        SATELLITE,
        AUXILIARY;

        public static final NodeType[] ALL_NODE_TYPES =
        {
            CONTROLLER,
            SATELLITE,
            AUXILIARY
        };
    }

    public enum NodeFlags implements Flags
    {
        REMOVE(1L),
        QIGNORE(0x10000L);

        public static final NodeFlags[] ALL_FLAGS =
        {
            REMOVE
        };

        public final long flagValue;

        private NodeFlags(long value)
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
