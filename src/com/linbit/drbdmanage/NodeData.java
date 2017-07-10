package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsBits;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NodeData extends BaseTransactionObject implements Node
{
    // Object identifier
    private UUID objId;

    // Node name
    private NodeName clNodeName;

    // State flags
    private StateFlags<NodeFlag> flags;

    // Node type
    private StateFlags<NodeType> nodeTypeFlags;

    // List of resources assigned to this cluster node
    private Map<ResourceName, Resource> resourceMap;

    // List of network interfaces used for replication on this cluster node
    private Map<NetInterfaceName, NetInterface> netInterfaceMap;

    // List of storage pools
    private Map<StorPoolName, StorPool> storPoolMap;

    // Access controls for this object
    private ObjectProtection objProt;

    // Properties container for this node
    private Props nodeProps;

    private NodeDataDatabaseDriver dbDriver;

    private final List<TransactionObject> transObjList;

    /*
     * Only used by getInstance method
     */
    private NodeData(
        AccessContext accCtx,
        NodeName nameRef,
        Set<NodeType> types,
        Set<NodeFlag> flagSet,
        SerialGenerator srlGen,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            ObjectProtection.getInstance(
                accCtx,
                transMgr,
                ObjectProtection.buildPath(nameRef),
                true
            ),
            nameRef,
            types,
            flagSet,
            srlGen,
            transMgr
        );
    }

    /*
     * Used by dbDrivers and tests
     */
    NodeData(
        UUID uuidRef,
        ObjectProtection objProtRef,
        NodeName nameRef,
        Set<NodeType> types,
        Set<NodeFlag> flagSet,
        SerialGenerator srlGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        ErrorCheck.ctorNotNull(NodeData.class, NodeName.class, nameRef);

        objId = uuidRef;
        objProt = objProtRef;
        clNodeName = nameRef;
        dbDriver = DrbdManage.getNodeDataDatabaseDriver(nameRef);

        resourceMap = new TreeMap<ResourceName, Resource>();
        netInterfaceMap = new TreeMap<NetInterfaceName, NetInterface>();
        storPoolMap = new TreeMap<StorPoolName, StorPool>();

        nodeProps = SerialPropsContainer.loadContainer(dbDriver.getPropsConDriver(), transMgr, srlGen);


        long initialFlags = 0;
        if (flagSet != null)
        {
            initialFlags = StateFlagsBits.getMask(flagSet.toArray(new NodeFlag[0]));
        }
        flags = new NodeFlagsImpl(objProt, dbDriver.getStateFlagPersistence(), initialFlags);

        long initialTypes = 0;
        if (types == null || types.isEmpty())
        {
            // Default to creating an AUXILIARY type node
            initialTypes = NodeType.AUXILIARY.getFlagValue();
        }
        else
        {
            initialTypes = StateFlagsBits.getMask(types.toArray(new NodeType[0]));
        }
        nodeTypeFlags = new NodeTypesFlagsImpl(objProt, dbDriver.getNodeTypeStateFlagPersistence(), initialTypes);

        transObjList = Arrays.<TransactionObject> asList(
            flags,
            nodeTypeFlags,
            objProt,
            nodeProps
        );

        if (transMgr != null)
        {
            setConnection(transMgr);
        }
    }

    public static NodeData getInstance(
        AccessContext accCtx,
        NodeName nameRef,
        Set<NodeType> types,
        Set<NodeFlag> flags,
        SerialGenerator srlGen,
        TransactionMgr transMgr,
        boolean createIfNotExists
    )
        throws SQLException, AccessDeniedException
    {
        NodeData nodeData = null;

        NodeDataDatabaseDriver dbDriver = DrbdManage.getNodeDataDatabaseDriver(nameRef);
        if (transMgr != null)
        {
            nodeData = dbDriver.load(transMgr.dbCon, srlGen, transMgr);
        }

        if (nodeData != null)
        {
            nodeData.objProt.requireAccess(accCtx, AccessType.CONTROL);
            nodeData.setConnection(transMgr);
        }
        else
        if (createIfNotExists)
        {
            nodeData = new NodeData(accCtx, nameRef, types, flags, srlGen, transMgr);
            if (transMgr != null)
            {
                dbDriver.create(transMgr.dbCon, nodeData);
            }
        }

        if (nodeData != null)
        {
            nodeData.initialized();
        }
        return nodeData;
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
    public StateFlags<NodeFlag> getFlags()
    {
        return flags;
    }

    @Override
    public void initialized()
    {
        for (TransactionObject transObj : transObjList)
        {
            transObj.initialized();
        }
    }

    @Override
    public void setConnection(TransactionMgr transMgr) throws ImplementationError
    {
        if (transMgr != null)
        {
            transMgr.register(this);
        }
        for (TransactionObject transObj : transObjList)
        {
            transObj.setConnection(transMgr);
        }
    }

    @Override
    public void commit()
    {
        for (TransactionObject transObj : transObjList)
        {
            transObj.commit();
        }
    }

    @Override
    public void rollback()
    {
        for (TransactionObject transObj : transObjList)
        {
            transObj.rollback();
        }
    }

     @Override
    public boolean isDirty()
    {
        boolean dirty = false;
        for (TransactionObject transObj : transObjList)
        {
            if (transObj.isDirty())
            {
                dirty = true;
                break;
            }
        }
        return dirty;
    }

    private static final class NodeFlagsImpl extends StateFlagsBits<NodeFlag>
    {
        NodeFlagsImpl(ObjectProtection objProtRef, StateFlagsPersistence persistenceRef, long initialFlags)
        {
            super(objProtRef, StateFlagsBits.getMask(NodeFlag.values()), persistenceRef, initialFlags);
        }
    }

    private static final class NodeTypesFlagsImpl extends StateFlagsBits<NodeType>
    {
        NodeTypesFlagsImpl(ObjectProtection objProtRef, StateFlagsPersistence persistenceRef, long initialFlags)
        {
            super(objProtRef, StateFlagsBits.getMask(NodeType.values()), persistenceRef, initialFlags);
        }
    }
}
