package com.linbit.linstor.core.objects;

import static java.util.stream.Collectors.toList;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.NodePojo.NodeConnPojo;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.dbdrivers.DatabaseException;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

import reactor.core.publisher.FluxSink;

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

    // List of snapshots on this cluster node
    private final TransactionMap<SnapshotDefinition.Key, Snapshot> snapshotMap;

    // List of network interfaces used for replication on this cluster node
    private final TransactionMap<NetInterfaceName, NetInterface> netInterfaceMap;

    // List of storage pools
    private final TransactionMap<StorPoolName, StorPool> storPoolMap;

    // Map to the other endpoint of a node connection (this is NOT necessarily the source!)
    private final TransactionMap<NodeName, NodeConnection> nodeConnections;

    // Access controls for this object
    private final ObjectProtection objProt;

    // Properties container for this node
    private final Props nodeProps;

    private final NodeDataDatabaseDriver dbDriver;

    private transient Peer peer;

    private transient TransactionSimpleObject<NodeData, NetInterface> activeStltConn;

    private final TransactionSimpleObject<NodeData, Boolean> deleted;

    private FluxSink<Boolean> initialConnectSink;

    NodeData(
        UUID uuidRef,
        ObjectProtection objProtRef,
        NodeName nameRef,
        NodeType type,
        long initialFlags,
        NodeDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
        throws DatabaseException
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
        Provider<? extends TransactionMgr> transMgrProvider,
        Map<ResourceName, Resource> rscMapRef,
        Map<SnapshotDefinition.Key, Snapshot> snapshotMapRef,
        Map<NetInterfaceName, NetInterface> netIfMapRef,
        Map<StorPoolName, StorPool> storPoolMapRef,
        Map<NodeName, NodeConnection> nodeConnMapRef
    )
        throws DatabaseException
    {
        super(transMgrProvider);
        ErrorCheck.ctorNotNull(NodeData.class, NodeName.class, nameRef);

        objId = uuidRef;
        dbgInstanceId = UUID.randomUUID();
        objProt = objProtRef;
        clNodeName = nameRef;
        dbDriver = dbDriverRef;

        resourceMap = transObjFactory.createTransactionMap(rscMapRef, null);
        snapshotMap = transObjFactory.createTransactionMap(snapshotMapRef, null);
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

        activeStltConn = transObjFactory.createTransactionSimpleObject(this, null, null);
        transObjs = Arrays.<TransactionObject>asList(
            flags,
            nodeType,
            objProt,
            resourceMap,
            snapshotMap,
            netInterfaceMap,
            storPoolMap,
            nodeConnections,
            nodeProps,
            deleted, activeStltConn
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
        return nodeConnections.get(otherNode.getName());
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
            nodeConnections.put(targetNode.getName(), nodeConnection);
        }
        else
        {
            nodeConnections.put(sourceNode.getName(), nodeConnection);
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
            nodeConnections.remove(targetNode.getName());
        }
        else
        {
            nodeConnections.remove(sourceNode.getName());
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
    public void addSnapshot(AccessContext accCtx, Snapshot snapshot)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        snapshotMap.put(new SnapshotDefinition.Key(snapshot.getSnapshotDefinition()), snapshot);
    }

    @Override
    public void removeSnapshot(SnapshotData snapshotData)
    {
        checkDeleted();
        snapshotMap.remove(new SnapshotDefinition.Key(snapshotData.getSnapshotDefinition()));
    }

    @Override
    public boolean hasSnapshots(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return !snapshotMap.isEmpty();
    }

    @Override
    public Collection<Snapshot> getInProgressSnapshots(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        List<Snapshot> inProgressSnapshots = new ArrayList<>();
        for (Snapshot snapshot : snapshotMap.values())
        {
            if (snapshot.getSnapshotDefinition().getInProgress(accCtx))
            {
                inProgressSnapshots.add(snapshot);
            }
        }
        return inProgressSnapshots;
    }

    @Override
    public Collection<Snapshot> getSnapshots(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return snapshotMap.values();
    }

    @Override
    public NetInterface getNetInterface(AccessContext accCtx, NetInterfaceName niName) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return netInterfaceMap.get(niName);
    }

    public void addNetInterface(AccessContext accCtx, NetInterface niRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        netInterfaceMap.put(niRef.getName(), niRef);
    }

    void removeNetInterface(AccessContext accCtx, NetInterface niRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        netInterfaceMap.remove(niRef.getName());

        if (Objects.equals(activeStltConn.get(), niRef))
        {
            removeActiveSatelliteconnection(accCtx);
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

    public void addStorPool(AccessContext accCtx, StorPool pool)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        storPoolMap.put(pool.getName(), pool);
    }

    public void removeStorPool(AccessContext accCtx, StorPool pool)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        storPoolMap.remove(pool.getName());
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
        throws AccessDeniedException, DatabaseException
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
    public NetInterface getActiveStltConn(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return activeStltConn.get();
    }

    @Override
    public void setActiveStltConn(AccessContext accCtx, NetInterface satelliteConnectionRef)
        throws AccessDeniedException, DatabaseException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        activeStltConn.set(satelliteConnectionRef);
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

    void removeActiveSatelliteconnection(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        activeStltConn.set(null);
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
    public void markDeleted(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CONTROL);
        getFlags().enableFlags(accCtx, NodeFlag.DELETE);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            if (!resourceMap.isEmpty())
            {
                throw new ImplementationError("Node with resources cannot be deleted");
            }

            if (!snapshotMap.isEmpty())
            {
                throw new ImplementationError("Node with snapshots cannot be deleted");
            }

            // Shallow copy the collection because elements may be removed from it
            ArrayList<NodeConnection> values = new ArrayList<>(nodeConnections.values());
            for (NodeConnection nodeConn : values)
            {
                nodeConn.delete(accCtx);
            }

            // Shallow copy the collection because elements may be removed from it
            ArrayList<StorPool> storPools = new ArrayList<>(storPoolMap.values());
            for (StorPool storPool : storPools)
            {
                storPool.delete(accCtx);
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
                    nodeConn.getProps(accCtx).map()
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
            activeStltConn.get() != null ? activeStltConn.get().getApiData(accCtx) : null,
            nodeConns,
            getProps(accCtx).map(),
            tmpPeer != null ? tmpPeer.getConnectionStatus() : Peer.ConnectionStatus.UNKNOWN,
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

    public void registerInitialConnectSink(FluxSink<Boolean> fluxSinkRef)
    {
        if (initialConnectSink != null)
        {
            throw new ImplementationError("Only one initialConnectSink allowed");
        }
        initialConnectSink = fluxSinkRef;
    }

    @Override
    public boolean connectionEstablished()
    {
        boolean ret = false;
        if (initialConnectSink != null)
        {
            initialConnectSink.next(true);
            initialConnectSink.complete();
            initialConnectSink = null;
            ret = true;
        }
        return ret;
    }
}
