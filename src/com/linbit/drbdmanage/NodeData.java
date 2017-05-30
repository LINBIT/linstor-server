package com.linbit.drbdmanage;

import com.linbit.ErrorCheck;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import com.linbit.drbdmanage.Node.NodeFlags;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsBits;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NodeData implements Node
{
    // Object identifier
    private UUID objId;

    // Node name
    private NodeName clNodeName;

    // Node type
    private Set<NodeType> nodeTypeList;

    // List of resources assigned to this cluster node
    private Map<ResourceName, Resource> resourceMap;

    // List of network interfaces used for replication on this cluster node
    private Map<NetInterfaceName, NetInterface> netInterfaceMap;

    // List of storage pools
    private Map<StorPoolName, StorPool> storPoolMap;

    // State flags
    private StateFlags<NodeFlags> flags;

    // Access controls for this object
    private ObjectProtection objProt;

    // Properties container for this node
    private Props nodeProps;

    NodeData(AccessContext accCtx, NodeName nameRef, Set<NodeType> types, SerialGenerator srlGen)
    {
        ErrorCheck.ctorNotNull(NodeData.class, NodeName.class, nameRef);
        ErrorCheck.ctorNotNull(NodeData.class, NodeType.class, types);
        objId = UUID.randomUUID();
        clNodeName = nameRef;
        nodeTypeList = new TreeSet<>();
        nodeTypeList.addAll(types);
        // Default to creating an AUXILIARY type node
        if (nodeTypeList.isEmpty())
        {
            nodeTypeList.add(NodeType.AUXILIARY);
        }
        resourceMap = new TreeMap<>();
        netInterfaceMap = new TreeMap<>();
        storPoolMap = new TreeMap<>();
        nodeProps = SerialPropsContainer.createRootContainer(srlGen);
        objProt = new ObjectProtection(accCtx);
        flags = new NodeFlagsImpl(objProt);
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public NodeName getName()
    {
        return clNodeName;
    }

    @Override
    public Resource getResource(AccessContext accCtx, ResourceName resName)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.get(resName);
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return objProt;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        return PropsAccess.secureGetProps(accCtx, objProt, nodeProps);
    }

    @Override
    public void addResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.put(resRef.getDefinition().getName(), resRef);
    }

    @Override
    public void removeResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.remove(resRef.getDefinition().getName());
    }

    @Override
    public Iterator<Resource> iterateResources(AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return resourceMap.values().iterator();
    }

    @Override
    public NetInterface getNetInterface(AccessContext accCtx, NetInterfaceName niName) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return netInterfaceMap.get(niName);
    }

    @Override
    public void addNetInterface(AccessContext accCtx, NetInterface niRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        netInterfaceMap.put(niRef.getName(), niRef);
    }

    @Override
    public void removeNetInterface(AccessContext accCtx, NetInterface niRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        netInterfaceMap.remove(niRef.getName());
    }

    @Override
    public Iterator<NetInterface> iterateNetInterfaces(AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return netInterfaceMap.values().iterator();
    }

    @Override
    public StorPool getStorPool(AccessContext accCtx, StorPoolName poolName)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return storPoolMap.get(poolName);
    }

    @Override
    public void addStorPool(AccessContext accCtx, StorPool pool)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        storPoolMap.put(pool.getName(), pool);
    }

    @Override
    public void removeStorPool(AccessContext accCtx, StorPool pool)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        storPoolMap.remove(pool.getName());
    }

    @Override
    public Iterator<StorPool> iterateStorPools(AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return storPoolMap.values().iterator();
    }

    @Override
    public Iterator<NodeType> iterateNodeTypes(AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return Collections.unmodifiableSet(nodeTypeList).iterator();
    }

    @Override
    public boolean hasNodeType(AccessContext accCtx, NodeType reqType)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return (nodeTypeList.contains(reqType));
    }

    @Override
    public StateFlags<NodeFlags> getFlags()
    {
        return flags;
    }

    private static final class NodeFlagsImpl extends StateFlagsBits<NodeFlags>
    {
        NodeFlagsImpl(ObjectProtection objProtRef)
        {
            super(objProtRef, StateFlagsBits.getMask(NodeFlags.ALL_FLAGS));
        }
    }
}
