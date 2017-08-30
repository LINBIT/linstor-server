package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
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
    private final UUID objId;

    // Node name
    private final NodeName clNodeName;

    // State flags
    private final StateFlags<NodeFlag> flags;

    // Node type
    private final StateFlags<NodeType> nodeTypeFlags;

    // List of resources assigned to this cluster node
    private final Map<ResourceName, Resource> resourceMap;

    // List of network interfaces used for replication on this cluster node
    private final Map<NetInterfaceName, NetInterface> netInterfaceMap;

    // List of storage pools
    private final Map<StorPoolName, StorPool> storPoolMap;

    // Access controls for this object
    private final ObjectProtection objProt;

    // Properties container for this node
    private final Props nodeProps;

    private final NodeDataDatabaseDriver dbDriver;

    private boolean deleted = false;

    /*
     * Only used by getInstance method
     */
    private NodeData(
        AccessContext accCtx,
        NodeName nameRef,
        long initialTypes,
        long initialFlags,
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
            initialTypes,
            initialFlags,
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
        long initialTypes,
        long initialFlags,
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

        resourceMap = new TreeMap<>();
        netInterfaceMap = new TreeMap<>();
        storPoolMap = new TreeMap<>();

        nodeProps = SerialPropsContainer.getInstance(dbDriver.getPropsConDriver(), transMgr, srlGen);

        flags = new NodeFlagsImpl(objProt, dbDriver.getStateFlagPersistence(), initialFlags);
        if (initialTypes == 0)
        {
            // Default to creating an AUXILIARY type node
            initialTypes = NodeType.AUXILIARY.getFlagValue();
        }
        nodeTypeFlags = new NodeTypesFlagsImpl(objProt, dbDriver.getNodeTypeStateFlagPersistence(), initialTypes);

        transObjs = Arrays.<TransactionObject> asList(
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
        NodeType[] types,
        NodeFlag[] flags,
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
            nodeData = dbDriver.load(srlGen, transMgr);
        }

        if (nodeData != null)
        {
            nodeData.objProt.requireAccess(accCtx, AccessType.CONTROL);
            nodeData.setConnection(transMgr);
        }
        else
        if (createIfNotExists)
        {
            nodeData = new NodeData(
                accCtx,
                nameRef,
                StateFlagsBits.getMask(types),
                StateFlagsBits.getMask(flags),
                srlGen,
                transMgr
            );
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
        checkDeleted();
        return objId;
    }

    @Override
    public NodeName getName()
    {
        checkDeleted();
        return clNodeName;
    }

    @Override
    public Resource getResource(AccessContext accCtx, ResourceName resName)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.get(resName);
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, objProt, nodeProps);
    }

    void addResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.put(resRef.getDefinition().getName(), resRef);
    }

    void removeResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.remove(resRef.getDefinition().getName());
        // TODO: gh - if a resource is removed from the map, should we "invalidate" the resource?
        // should we also update the database to remove the resource from the db?
    }

    @Override
    public Iterator<Resource> iterateResources(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return resourceMap.values().iterator();
    }

    @Override
    public NetInterface getNetInterface(AccessContext accCtx, NetInterfaceName niName) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return netInterfaceMap.get(niName);
    }

    void addNetInterface(AccessContext accCtx, NetInterface niRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        netInterfaceMap.put(niRef.getName(), niRef);
    }

    void removeNetInterface(AccessContext accCtx, NetInterface niRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        netInterfaceMap.remove(niRef.getName());
    }

    @Override
    public Iterator<NetInterface> iterateNetInterfaces(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return netInterfaceMap.values().iterator();
    }

    @Override
    public StorPool getStorPool(AccessContext accCtx, StorPoolName poolName)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return storPoolMap.get(poolName);
    }

    void addStorPool(AccessContext accCtx, StorPool pool)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        storPoolMap.put(pool.getName(), pool);
    }

    void removeStorPool(AccessContext accCtx, StorPool pool)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        storPoolMap.remove(pool.getName());
    }

    @Override
    public Iterator<StorPool> iterateStorPools(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return storPoolMap.values().iterator();
    }

    @Override
    public long getNodeTypes(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return nodeTypeFlags.getFlagsBits(accCtx);
    }

    @Override
    public boolean hasNodeType(AccessContext accCtx, NodeType reqType)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return nodeTypeFlags.isSet(accCtx, reqType);
    }

    @Override
    public StateFlags<NodeFlag> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CONTROL);

        dbDriver.delete(dbCon);
        deleted = true;
    }

    private void checkDeleted()
    {
        if (deleted)
        {
            throw new ImplementationError("Access to deleted node", null);
        }
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
