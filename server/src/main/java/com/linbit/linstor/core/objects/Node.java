package com.linbit.linstor.core.objects;

import static java.util.stream.Collectors.toList;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.NodePojo.NodeConnPojo;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.NodeDatabaseDriver;
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
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
public class Node extends BaseTransactionObject
    implements DbgInstanceUuid, Comparable<Node>, ProtectedObject
{
    public static interface InitMaps
    {
        Map<ResourceName, Resource> getRscMap();
        Map<SnapshotDefinition.Key, Snapshot> getSnapshotMap();
        Map<NetInterfaceName, NetInterface> getNetIfMap();
        Map<StorPoolName, StorPool> getStorPoolMap();
        Map<NodeName, NodeConnection> getNodeConnMap();
    }

    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Node name
    private final NodeName clNodeName;

    // State flags
    private final StateFlags<Flags> flags;

    // Node type
    private final TransactionSimpleObject<Node, Type> nodeType;

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

    private final NodeDatabaseDriver dbDriver;

    private transient Peer peer;

    private transient TransactionSimpleObject<Node, NetInterface> activeStltConn;

    private final TransactionSimpleObject<Node, Boolean> deleted;

    private FluxSink<Boolean> initialConnectSink;

    Node(
        UUID uuidRef,
        ObjectProtection objProtRef,
        NodeName nameRef,
        Type type,
        long initialFlags,
        NodeDatabaseDriver dbDriverRef,
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

    Node(
        UUID uuidRef,
        ObjectProtection objProtRef,
        NodeName nameRef,
        Type type,
        long initialFlags,
        NodeDatabaseDriver dbDriverRef,
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
        ErrorCheck.ctorNotNull(Node.class, NodeName.class, nameRef);

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
            Flags.class,
            dbDriver.getStateFlagPersistence(),
            initialFlags
        );

        // Default to creating an AUXILIARY type node
        Type checkedType = type == null ? Type.AUXILIARY : type;
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

    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    public NodeName getName()
    {
        checkDeleted();
        return clNodeName;
    }

    public Resource getResource(AccessContext accCtx, ResourceName resName)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.get(resName);
    }


    public NodeConnection getNodeConnection(AccessContext accCtx, Node otherNode)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        otherNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return nodeConnections.get(otherNode.getName());
    }


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


    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, objProt, nodeProps);
    }


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


    public int getResourceCount()
    {
        return resourceMap.size();
    }


    public Iterator<Resource> iterateResources(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return resourceMap.values().iterator();
    }


    public Stream<Resource> streamResources(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return resourceMap.values().stream();
    }


    public void addSnapshot(AccessContext accCtx, Snapshot snapshot)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        snapshotMap.put(new SnapshotDefinition.Key(snapshot.getSnapshotDefinition()), snapshot);
    }


    public void removeSnapshot(Snapshot snapshot)
    {
        checkDeleted();
        snapshotMap.remove(new SnapshotDefinition.Key(snapshot.getSnapshotDefinition()));
    }


    public boolean hasSnapshots(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return !snapshotMap.isEmpty();
    }


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


    public Collection<Snapshot> getSnapshots(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return snapshotMap.values();
    }

    public Iterator<Snapshot> iterateSnapshots(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return snapshotMap.values().iterator();
    }

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


    public Iterator<NetInterface> iterateNetInterfaces(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return netInterfaceMap.values().iterator();
    }


    public Stream<NetInterface> streamNetInterfaces(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return netInterfaceMap.values().stream();
    }


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


    public int getStorPoolCount()
    {
        return storPoolMap.size();
    }


    public Iterator<StorPool> iterateStorPools(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return storPoolMap.values().iterator();
    }


    public Stream<StorPool> streamStorPools(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return storPoolMap.values().stream();
    }


    public void copyStorPoolMap(AccessContext accCtx, Map<? super StorPoolName, ? super StorPool> dstMap)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        dstMap.putAll(storPoolMap);
    }

    public Type setNodeType(AccessContext accCtx, Type newType)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        return nodeType.set(newType);
    }


    public Type getNodeType(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return nodeType.get();
    }


    public boolean hasNodeType(AccessContext accCtx, Type reqType)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);

        long reqFlags = reqType.getFlagValue();
        return (nodeType.get().getFlagValue() & reqFlags) == reqFlags;
    }


    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }


    public Peer getPeer(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return peer;
    }


    public void setPeer(AccessContext accCtx, Peer peerRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        peer = peerRef;
    }


    public NetInterface getActiveStltConn(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return activeStltConn.get();
    }


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


    public void markDeleted(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CONTROL);
        getFlags().enableFlags(accCtx, Flags.DELETE);
    }


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
            ArrayList<NetInterface> netIfs = new ArrayList<>(netInterfaceMap.values());
            for (NetInterface netIf : netIfs)
            {
                netIf.delete(accCtx);
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


    public NodeApi getApiData(
        AccessContext accCtx,
        Long fullSyncId,
        Long updateId
    )
        throws AccessDeniedException
    {
        List<NetInterfaceApi> netInterfaces = new ArrayList<>();
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
        ExtToolsManager extToolsManager = peer.getExtToolsManager();

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
            updateId,
            extToolsManager.getSupportedLayers().stream()
                .map(deviceLayerKind -> deviceLayerKind.name()).collect(toList()),
            extToolsManager.getSupportedProviders().stream()
                .map(deviceProviderKind -> deviceProviderKind.name()).collect(toList()),
            extToolsManager.getUnsupportedLayersWithReasonsAsString(),
            extToolsManager.getUnsupportedProvidersWithReasonsAsString()
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

    /**
     * @return true iff this is the initial connection attempt
     */
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

    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        DELETE(1L),
        QIGNORE(0x10000L);

        public final long flagValue;

        Flags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static Flags[] valuesOfIgnoreCase(String string)
        {
            Flags[] flags;
            if (string == null)
            {
                flags = new Flags[0];
            }
            else
            {
                String[] split = string.split(",");
                flags = new Flags[split.length];

                for (int idx = 0; idx < split.length; idx++)
                {
                    flags[idx] = Flags.valueOf(split[idx].toUpperCase().trim());
                }
            }
            return flags;
        }

        public static Flags[] restoreFlags(long nodeFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((nodeFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new Flags[flagList.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(Flags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(Flags.class, listFlags);
        }
    }

    public enum Type implements com.linbit.linstor.stateflags.Flags
    {
        CONTROLLER(1, Collections.emptyList()),
        SATELLITE(
            2,
            Arrays.asList(
                DeviceProviderKind.DISKLESS,
                DeviceProviderKind.LVM,
                DeviceProviderKind.LVM_THIN,
                DeviceProviderKind.ZFS,
                DeviceProviderKind.ZFS_THIN,
                DeviceProviderKind.FILE,
                DeviceProviderKind.FILE_THIN,
                DeviceProviderKind.SPDK
            )
        ),
        COMBINED(
            3,
            Arrays.asList(
                DeviceProviderKind.DISKLESS,
                DeviceProviderKind.LVM,
                DeviceProviderKind.LVM_THIN,
                DeviceProviderKind.ZFS,
                DeviceProviderKind.ZFS_THIN,
                DeviceProviderKind.FILE,
                DeviceProviderKind.FILE_THIN,
                DeviceProviderKind.SPDK
            )
        ),
        AUXILIARY(
            4, Collections.emptyList()
        );

        private final int flag;
        private final List<DeviceProviderKind> allowedKindClasses;

        Type(int flagValue, List<DeviceProviderKind> allowedKindClassesRef)
        {

            flag = flagValue;
            allowedKindClasses = Collections.unmodifiableList(allowedKindClassesRef);
        }

        @Override
        public long getFlagValue()
        {
            return flag;
        }

        public static Type getByValue(long value)
        {
            Type ret = null;
            for (Type type : Type.values())
            {
                if (type.flag == value)
                {
                    ret = type;
                    break;
                }
            }
            return ret;
        }

        public static Type valueOfIgnoreCase(String string, Type defaultValue)
            throws IllegalArgumentException
        {
            Type ret = defaultValue;
            if (string != null)
            {
                Type val = valueOf(string.toUpperCase());
                if (val != null)
                {
                    ret = val;
                }
            }
            return ret;
        }

        public List<DeviceProviderKind> getAllowedKindClasses()
        {
            return allowedKindClasses;
        }

        public boolean isDeviceProviderKindAllowed(DeviceProviderKind kindRef)
        {
            return allowedKindClasses.contains(kindRef);
        }
    }
}
