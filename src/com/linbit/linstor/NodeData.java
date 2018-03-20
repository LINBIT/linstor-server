package com.linbit.linstor;

import static java.util.stream.Collectors.toList;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.NodePojo.NodeConnPojo;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NodeData extends BaseTransactionObject implements Node
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Node name
    private final NodeName clNodeName;

    // State flags
    private final StateFlags<NodeFlag> flags;

    // Node type
    private final TransactionSimpleObject<NodeData, NodeType> nodeType;

    // List of resources assigned to this cluster node
    private final TransactionMap<ResourceName, Resource> resourceMap;

    // List of network interfaces used for replication on this cluster node
    private final TransactionMap<NetInterfaceName, NetInterface> netInterfaceMap;

    // List of storage pools
    private final TransactionMap<StorPoolName, StorPool> storPoolMap;

    // Map to the other endpoint of a node connection (this is NOT necessarily the source!)
    private final TransactionMap<Node, NodeConnection> nodeConnections;

    // Access controls for this object
    private final ObjectProtection objProt;

    // Properties container for this node
    private final Props nodeProps;

    private final NodeDataDatabaseDriver dbDriver;

    private transient Peer peer;

    private transient TransactionSimpleObject<NodeData, NetInterface> currentStltConn;

    private TransactionSimpleObject<NodeData, Boolean> deleted;

    private transient StorPoolData disklessStorPool;

    NodeData(
        UUID uuidRef,
        ObjectProtection objProtRef,
        NodeName nameRef,
        NodeType type,
        long initialFlags,
        NodeDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
        throws SQLException
    {
        this(
            uuidRef,
            objProtRef,
            nameRef,
            type,
            initialFlags,
            dbDriverRef,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>(),
            new TreeMap<>(),
            new TreeMap<>()
        );

    }

    NodeData(
        UUID uuidRef,
        ObjectProtection objProtRef,
        NodeName nameRef,
        NodeType type,
        long initialFlags,
        NodeDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider,
        Map<ResourceName, Resource> rscMapRef,
        Map<NetInterfaceName, NetInterface> netIfMapRef,
        Map<StorPoolName, StorPool> storPoolMapRef,
        Map<Node, NodeConnection> nodeConnMapRef
    )
        throws SQLException
    {
        super(transMgrProvider);
        ErrorCheck.ctorNotNull(NodeData.class, NodeName.class, nameRef);

        objId = uuidRef;
        dbgInstanceId = UUID.randomUUID();
        objProt = objProtRef;
        clNodeName = nameRef;
        dbDriver = dbDriverRef;

        resourceMap = transObjFactory.createTransactionMap(rscMapRef, null);
        netInterfaceMap = transObjFactory.createTransactionMap(netIfMapRef, null);
        storPoolMap = transObjFactory.createTransactionMap(storPoolMapRef, null);
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        nodeProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(nameRef)
        );
        nodeConnections = transObjFactory.createTransactionMap(nodeConnMapRef, null);

        flags = transObjFactory.createStateFlagsImpl(
            objProt,
            this,
            NodeFlag.class,
            dbDriver.getStateFlagPersistence(),
            initialFlags
        );

        // Default to creating an AUXILIARY type node
        NodeType checkedType = type == null ? NodeType.AUXILIARY : type;
        nodeType = transObjFactory.createTransactionSimpleObject(
            this, checkedType, dbDriver.getNodeTypeDriver()
        );

        currentStltConn = transObjFactory.createTransactionSimpleObject(this, null, null);
        transObjs = Arrays.<TransactionObject>asList(
            flags,
            nodeType,
            objProt,
            resourceMap,
            netInterfaceMap,
            storPoolMap,
            nodeConnections,
            nodeProps,
            deleted,
            currentStltConn
        );
    }

    @Override
    public int compareTo(Node node)
    {
        return this.getName().compareTo(node.getName());
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
    public NodeConnection getNodeConnection(AccessContext accCtx, Node otherNode)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        otherNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return nodeConnections.get(otherNode);
    }

    @Override
    public void setNodeConnection(AccessContext accCtx, NodeConnection nodeConnection)
        throws AccessDeniedException
    {
        checkDeleted();
        Node sourceNode = nodeConnection.getSourceNode(accCtx);
        Node targetNode = nodeConnection.getTargetNode(accCtx);

        sourceNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        if (sourceNode == this)
        {
            nodeConnections.put(targetNode, nodeConnection);
        }
        else
        {
            nodeConnections.put(sourceNode, nodeConnection);
        }
    }

    @Override
    public void removeNodeConnection(AccessContext accCtx, NodeConnection nodeConnection)
        throws AccessDeniedException
    {
        checkDeleted();

        Node sourceNode = nodeConnection.getSourceNode(accCtx);
        Node targetNode = nodeConnection.getTargetNode(accCtx);

        sourceNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        if (sourceNode == this)
        {
            nodeConnections.remove(targetNode);
        }
        else
        {
            nodeConnections.remove(sourceNode);
        }
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

    @Override
    public void addResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
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
    }

    @Override
    public int getResourceCount()
    {
        return resourceMap.size();
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
    public Stream<Resource> streamResources(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return resourceMap.values().stream();
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

    void removeNetInterface(AccessContext accCtx, NetInterface niRef) throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        netInterfaceMap.remove(niRef.getName());

        if (Objects.equals(currentStltConn.get(), niRef))
        {
            removeSatelliteconnection(accCtx);
        }
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
    public Stream<NetInterface> streamNetInterfaces(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return netInterfaceMap.values().stream();
    }

    @Override
    public StorPool getStorPool(AccessContext accCtx, StorPoolName poolName)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return storPoolMap.get(poolName);
    }

    @Override
    public StorPool getDisklessStorPool(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return disklessStorPool;
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

    void setDisklessStorPool(StorPoolData newDisklessStorPool)
    {
        disklessStorPool = newDisklessStorPool;
    }

    @Override
    public int getStorPoolCount()
    {
        return storPoolMap.size();
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
    public Stream<StorPool> streamStorPools(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return storPoolMap.values().stream();
    }

    @Override
    public void copyStorPoolMap(AccessContext accCtx, Map<? super StorPoolName, ? super StorPool> dstMap)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        dstMap.putAll(storPoolMap);
    }

    public NodeType setNodeType(AccessContext accCtx, NodeType newType)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        return nodeType.set(newType);
    }

    @Override
    public NodeType getNodeType(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return nodeType.get();
    }

    @Override
    public boolean hasNodeType(AccessContext accCtx, NodeType reqType)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        long reqFlags = reqType.getFlagValue();
        return (nodeType.get().getFlagValue() & reqFlags) == reqFlags;
    }

    @Override
    public StateFlags<NodeFlag> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public Peer getPeer(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return peer;
    }

    @Override
    public void setPeer(AccessContext accCtx, Peer peerRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        peer = peerRef;
    }

    @Override
    public NetInterface getSatelliteConnection(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return currentStltConn.get();
    }

    @Override
    public void setSatelliteConnection(AccessContext accCtx, NetInterface satelliteConnectionRef)
        throws AccessDeniedException, SQLException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        currentStltConn.set(satelliteConnectionRef);
        try
        {
            nodeProps.setProp(
                ApiConsts.KEY_CUR_STLT_CONN_NAME,
                satelliteConnectionRef.getName().displayValue
            );
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    void removeSatelliteconnection(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        currentStltConn.set(null);
        try
        {
            nodeProps.removeProp(ApiConsts.KEY_CUR_STLT_CONN_NAME);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public void markDeleted(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CONTROL);
        getFlags().enableFlags(accCtx, NodeFlag.DELETE);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            // preventing ConcurrentModificationException
            ArrayList<NodeConnection> values = new ArrayList<>(nodeConnections.values());
            for (NodeConnection nodeConn : values)
            {
                nodeConn.delete(accCtx);
            }

            nodeProps.delete();
            objProt.delete(accCtx);

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public boolean isDeleted()
    {
        return deleted.get();
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted node");
        }
    }

    @Override
    public NodeApi getApiData(
        AccessContext accCtx,
        Long fullSyncId,
        Long updateId
    )
        throws AccessDeniedException
    {
        List<NetInterface.NetInterfaceApi> netInterfaces = new ArrayList<>();
        for (NetInterface ni : streamNetInterfaces(accCtx).collect(toList()))
        {
            netInterfaces.add(ni.getApiData(accCtx));
        }

        List<NodeConnPojo> nodeConns = new ArrayList<>();
        for (NodeConnection nodeConn : nodeConnections.values())
        {
            Node otherNode;

            Node sourceNode = nodeConn.getSourceNode(accCtx);
            if (this.equals(sourceNode))
            {
                otherNode = nodeConn.getTargetNode(accCtx);
            }
            else
            {
                otherNode = sourceNode;
            }
            nodeConns.add(
                new NodeConnPojo(
                    nodeConn.getUuid(),
                    otherNode.getUuid(),
                    otherNode.getName().displayValue,
                    otherNode.getNodeType(accCtx).name(),
                    otherNode.getFlags().getFlagsBits(accCtx),
                    nodeConn.getProps(accCtx).map(),
                    otherNode.getDisklessStorPool(accCtx).getUuid()
                )
            );
        }

        Peer tmpPeer = getPeer(accCtx);

        return new NodePojo(
            getUuid(),
            getName().getDisplayName(),
            getNodeType(accCtx).name(),
            getFlags().getFlagsBits(accCtx),
            netInterfaces,
            nodeConns,
            getProps(accCtx).map(),
            tmpPeer != null && tmpPeer.isConnected(),
            disklessStorPool.getUuid(),
            fullSyncId,
            updateId
        );
    }

    @Override
    public String toString()
    {
        return "Node: '" + clNodeName + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }
}
