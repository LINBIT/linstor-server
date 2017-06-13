package com.linbit.drbdmanage;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.TransactionMap;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.UUID;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsBits;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

import java.net.InetAddress;
import java.util.Set;

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

    // State flags
    private StateFlags<NodeFlags> flags;

    // Node type
    private StateFlags<NodeType> nodeTypeFlags;

    // List of resources assigned to this cluster node
    private TransactionMap<ResourceName, Resource> resourceMap;

    // List of network interfaces used for replication on this cluster node
    private TransactionMap<NetInterfaceName, NetInterface> netInterfaceMap;

    // List of storage pools
    private TransactionMap<StorPoolName, StorPool> storPoolMap;

    // Access controls for this object
    private ObjectProtection objProt;

    // Properties container for this node
    private Props nodeProps;

    private NodeDatabaseDriver dbDriver;

    NodeData(AccessContext accCtx, NodeName nameRef, Set<NodeType> types, SerialGenerator srlGen)
        throws SQLException, AccessDeniedException
    {
        this(accCtx, nameRef, types, srlGen, null);
    }

    NodeData(AccessContext accCtx, NodeName nameRef, Set<NodeType> types, SerialGenerator srlGen, TransactionMgr transMgr)
        throws SQLException, AccessDeniedException
    {
        ErrorCheck.ctorNotNull(NodeData.class, NodeName.class, nameRef);
        ErrorCheck.ctorNotNull(NodeData.class, NodeType.class, types);
        objId = UUID.randomUUID();
        clNodeName = nameRef;

        dbDriver = DrbdManage.getNodeDatabaseDriver(nameRef);

        resourceMap = new TransactionMap<>(
            new TreeMap<ResourceName, Resource>(),
            dbDriver.getNodeResourceMapDriver()
        );
        netInterfaceMap = new TransactionMap<>(
            new TreeMap<NetInterfaceName, NetInterface>(),
            dbDriver.getNodeNetInterfaceMapDriver()
        );
        storPoolMap = new TransactionMap<>(
            new TreeMap<StorPoolName, StorPool>(),
            dbDriver.getNodeStorPoolMapDriver()
        );

        for (NodeType type : types)
        {
            nodeTypeFlags.enableFlags(accCtx, type);
        }

        // Default to creating an AUXILIARY type node
        if (!nodeTypeFlags.isSomeSet(accCtx, NodeType.ALL_NODE_TYPES))
        {
            nodeTypeFlags.enableFlags(accCtx, NodeType.AUXILIARY);
        }
        nodeProps = SerialPropsContainer.createRootContainer(srlGen);
        objProt = ObjectProtection.create(
            ObjectProtection.buildPath(this),
            accCtx,
            transMgr
        );
        flags = new NodeFlagsImpl(objProt, dbDriver.getStateFlagPersistence());
        nodeTypeFlags = new NodeTypesFlagsImpl(objProt, dbDriver.getNodeTypeStateFlagPersistence());
    }

    // TODO add static load function

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

    // TODO: instead of addNetInterface, what about a createNetInterface (which creates a netInterface
    // + passes it a node-specific db-driver to persist the address)
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

        storPoolMap.put(pool.getName(), pool);
    }

    @Override
    public Iterator<StorPool> iterateStorPools(AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return storPoolMap.values().iterator();
    }

    @Override
    public long getNodeTypes(AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return nodeTypeFlags.getFlagsBits(accCtx);
    }

    @Override
    public boolean hasNodeType(AccessContext accCtx, NodeType reqType)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return nodeTypeFlags.isSet(accCtx, reqType);
    }

    @Override
    public StateFlags<NodeFlags> getFlags()
    {
        return flags;
    }

    @Override
    public ObjectDatabaseDriver<InetAddress> getNetInterfaceDriver(NetInterfaceName netInterfaceName)
    {
        return dbDriver.getNodeNetInterfaceDriver(netInterfaceName);
    }

    @Override
    public void commit()
    {
        // objId is unmodifiable
        // nodeName is unmodifiable

        // flags
        nodeTypeFlags.commit();
        resourceMap.commit();
        netInterfaceMap.commit();
        storPoolMap.commit();
        flags.commit();
        objProt.commit();
        nodeProps.commit();
    }

    @Override
    public void rollback()
    {
        nodeTypeFlags.rollback();
        resourceMap.rollback();
        netInterfaceMap.rollback();
        storPoolMap.rollback();
        flags.rollback();
        objProt.rollback();
        nodeProps.rollback();
    }

    @Override
    public void setConnection(TransactionMgr transMgr) throws ImplementationError
    {
        transMgr.register(this);
        dbDriver.setConnection(transMgr.dbCon);
    }

    @Override
    public boolean isDirty()
    {
        return nodeTypeFlags.isDirty() ||
            resourceMap.isDirty() ||
            netInterfaceMap.isDirty() ||
            storPoolMap.isDirty() ||
            flags.isDirty() ||
            objProt.isDirty() ||
            nodeProps.isDirty();
    }

    private static final class NodeFlagsImpl extends StateFlagsBits<NodeFlags>
    {
        NodeFlagsImpl(ObjectProtection objProtRef, StateFlagsPersistence persistenceRef)
        {
            super(objProtRef, StateFlagsBits.getMask(NodeFlags.ALL_FLAGS), persistenceRef);
        }
    }

    private static final class NodeTypesFlagsImpl extends StateFlagsBits<NodeType>
    {
        NodeTypesFlagsImpl(ObjectProtection objProtRef, StateFlagsPersistence persistenceRef)
        {
            super(objProtRef, StateFlagsBits.getMask(NodeType.ALL_NODE_TYPES), persistenceRef);
        }
    }
}
