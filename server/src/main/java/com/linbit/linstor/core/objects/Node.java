package com.linbit.linstor.core.objects;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiConsts.ConnectionStatus;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.NodePojo.NodeConnPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.linstor.interfaces.NodeInfo;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerController;
import com.linbit.linstor.netcom.PeerOffline;
import com.linbit.linstor.netcom.StltImplErrPeer;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.DynamicNumberPoolImpl;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.ProcCryptoEntry;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.utils.LocalInetAddresses;
import com.linbit.utils.StringUtils;

import jakarta.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

import reactor.core.publisher.FluxSink;

import static java.util.stream.Collectors.toList;

/**
 * Represents a node in the LINSTOR cluster.
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class Node extends AbsCoreObj<Node> implements NodeInfo
{
    public interface InitMaps
    {
        Map<ResourceName, Resource> getRscMap();
        Map<SnapshotDefinition.Key, Snapshot> getSnapshotMap();
        Map<NetInterfaceName, NetInterface> getNetIfMap();
        Map<StorPoolName, StorPool> getStorPoolMap();
        Map<NodeName, NodeConnection> getNodeConnMap();
    }

    private static final String TCP_PORT_ELEMENT_NAME = "TCP port";

    // we will load the ranges from the database, but if the database contains
    // invalid ranges (e.g. -1 for port), we will fall back to these defaults
    private static final int DEFAULT_TCP_PORT_MIN = 7000;
    private static final int DEFAULT_TCP_PORT_MAX = 7999;

    // Node name
    private final NodeName nodeName;

    // State flags
    private final StateFlags<Flags> flags;

    // Node type
    private final TransactionSimpleObject<Node, Type> nodeType;

    // List of resources assigned to this cluster node
    private final TransactionMap<Node, ResourceName, Resource> resourceMap;

    // List of snapshots on this cluster node
    private final TransactionMap<Node, SnapshotDefinition.Key, Snapshot> snapshotMap;

    // List of network interfaces used for replication on this cluster node
    private final TransactionMap<Node, NetInterfaceName, NetInterface> netInterfaceMap;

    // List of storage pools
    private final TransactionMap<Node, StorPoolName, StorPool> storPoolMap;

    // Map to the other endpoint of a node connection (this is NOT necessarily the source!)
    private final TransactionMap<Node, NodeName, NodeConnection> nodeConnections;

    // Properties container for this node
    private final Props nodeProps;
    private final ReadOnlyProps roNodeProps;

    private final NodeDatabaseDriver dbDriver;

    private final DynamicNumberPool tcpPortPool;

    private volatile Peer peer;

    private transient TransactionSimpleObject<Node, @Nullable NetInterface> activeStltConn;

    private final Map<Object, FluxSink<Boolean>> initialConnectSinkMap;

    private final ArrayList<ProcCryptoEntry> supportedCryptos;

    private @Nullable Long evictionTimstamp;
    private long reconnectAttemptCount = 0;

    Node(
        UUID uuidRef,
        NodeName nameRef,
        @Nullable Type type,
        long initialFlags,
        ReadOnlyProps ctrlPropsRef,
        ErrorReporter errorReporterRef,
        NodeDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider,
        boolean createStltImplErrPeer
    )
        throws DatabaseException
    {
        this(
            uuidRef,
            nameRef,
            type,
            initialFlags,
            ctrlPropsRef,
            errorReporterRef,
            dbDriverRef,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>(),
            new TreeMap<>(),
            new TreeMap<>(),
            new TreeMap<>(),
            createStltImplErrPeer
        );
    }

    Node(
        UUID uuidRef,
        NodeName nameRef,
        @Nullable Type type,
        long initialFlags,
        ReadOnlyProps ctrlPropsRef,
        ErrorReporter errorReporterRef,
        NodeDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider,
        Map<ResourceName, Resource> rscMapRef,
        Map<SnapshotDefinition.Key, Snapshot> snapshotMapRef,
        Map<NetInterfaceName, NetInterface> netIfMapRef,
        Map<StorPoolName, StorPool> storPoolMapRef,
        Map<NodeName, NodeConnection> nodeConnMapRef,
        boolean createStltImplErrPeer
    )
        throws DatabaseException
    {
        super(uuidRef, transObjFactory, transMgrProvider);
        ErrorCheck.ctorNotNull(Node.class, NodeName.class, nameRef);

        nodeName = nameRef;
        dbDriver = dbDriverRef;

        resourceMap = transObjFactory.createTransactionMap(this, rscMapRef, null);
        snapshotMap = transObjFactory.createTransactionMap(this, snapshotMapRef, null);
        netInterfaceMap = transObjFactory.createTransactionMap(this, netIfMapRef, null);
        storPoolMap = transObjFactory.createTransactionMap(this, storPoolMapRef, null);

        nodeProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(nameRef),
            toStringImpl(),
            LinStorObject.NODE
        );
        roNodeProps = new ReadOnlyPropsImpl(nodeProps);
        tcpPortPool = createTcpPortPool(errorReporterRef, ctrlPropsRef);

        nodeConnections = transObjFactory.createTransactionMap(this, nodeConnMapRef, null);

        flags = transObjFactory.createStateFlagsImpl(
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

        initialConnectSinkMap = new HashMap<>();

        transObjs = Arrays.<TransactionObject>asList(
            flags,
            nodeType,
            // tcpPortPool, // TODO: tcpPortPool does not implement TransactionObject!
            resourceMap,
            snapshotMap,
            netInterfaceMap,
            storPoolMap,
            nodeConnections,
            nodeProps,
            deleted,
            activeStltConn
        );

        supportedCryptos = new ArrayList<>();
        peer = createOfflinePeer(errorReporterRef, createStltImplErrPeer);
    }

    private DynamicNumberPool createTcpPortPool(ErrorReporter errorReporter, ReadOnlyProps ctrlPropsRef)
    {
        return new DynamicNumberPoolImpl(
            errorReporter,
            new PriorityProps(roNodeProps, ctrlPropsRef),
            ApiConsts.KEY_TCP_PORT_AUTO_RANGE,
            ApiConsts.KEY_TCP_PORTS_BLOCKED,
            nodeName + "'s " + TCP_PORT_ELEMENT_NAME,
            TcpPortNumber::tcpPortNrCheck,
            TcpPortNumber.PORT_NR_MAX,
            DEFAULT_TCP_PORT_MIN,
            DEFAULT_TCP_PORT_MAX
        );
    }

    @Override
    public int compareTo(Node node)
    {
        return this.getName().compareTo(node.getName());
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(nodeName);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof Node other)
        {
            other.checkDeleted();
            ret = Objects.equals(nodeName, other.nodeName);
        }
        return ret;
    }

    public NodeName getKey()
    {
        // deliberately no checkDeleted!
        return nodeName;
    }

    @Override
    public NodeName getName()
    {
        checkDeleted();
        return nodeName;
    }

    public @Nullable Resource getResource(ResourceName resName)
    {
        checkDeleted();
        return resourceMap.get(resName);
    }

    public @Nullable NodeConnection getNodeConnection(Node otherNode)
    {
        checkDeleted();
        return nodeConnections.get(otherNode.getName());
    }

    public Collection<NodeConnection> getNodeConnections()
    {
        checkDeleted();
        return nodeConnections.values();
    }

    public void setNodeConnection(NodeConnection nodeConnection)
    {
        checkDeleted();
        Node sourceNode = nodeConnection.getSourceNode();
        Node targetNode = nodeConnection.getTargetNode();


        if (this.equals(sourceNode))
        {
            nodeConnections.put(targetNode.getName(), nodeConnection);
        }
        else
        {
            nodeConnections.put(sourceNode.getName(), nodeConnection);
        }
    }

    public void removeNodeConnection(NodeConnection nodeConnection)
    {
        checkDeleted();

        Node sourceNode = nodeConnection.getSourceNode();
        Node targetNode = nodeConnection.getTargetNode();


        if (this.equals(sourceNode))
        {
            nodeConnections.remove(targetNode.getName());
        }
        else
        {
            nodeConnections.remove(sourceNode.getName());
        }
    }

    public Props getProps()
    {
        checkDeleted();
        return nodeProps;
    }

    @Override
    public ReadOnlyProps getReadOnlyProps()
    {
        checkDeleted();
        return roNodeProps;
    }

    public void addResource(Resource resRef)
    {
        checkDeleted();

        resourceMap.put(resRef.getResourceDefinition().getName(), resRef);
    }

    void removeResource(Resource resRef)
    {
        checkDeleted();

        resourceMap.remove(resRef.getResourceDefinition().getName());
    }

    public int getResourceCount()
    {
        checkDeleted();
        return resourceMap.size();
    }

    public Iterator<Resource> iterateResources()
    {
        checkDeleted();

        return resourceMap.values().iterator();
    }

    public Stream<Resource> streamResources()
    {
        checkDeleted();

        return resourceMap.values().stream();
    }

    public void addSnapshot(Snapshot snapshot)
    {
        checkDeleted();

        snapshotMap.put(snapshot.getSnapshotDefinition().getSnapDfnKey(), snapshot);
    }

    public void removeSnapshot(Snapshot snapshot)
    {
        checkDeleted();
        snapshotMap.remove(snapshot.getSnapshotDefinition().getSnapDfnKey());
    }

    public boolean hasSnapshots()
    {
        checkDeleted();
        return !snapshotMap.isEmpty();
    }

    public Collection<Snapshot> getSnapshots()
    {
        checkDeleted();

        return snapshotMap.values();
    }

    public Iterator<Snapshot> iterateSnapshots()
    {
        checkDeleted();

        return snapshotMap.values().iterator();
    }

    public @Nullable NetInterface getNetInterface(NetInterfaceName niName)
    {
        checkDeleted();

        return netInterfaceMap.get(niName);
    }

    public void addNetInterface(NetInterface niRef)
    {
        checkDeleted();

        netInterfaceMap.put(niRef.getName(), niRef);
    }

    public void removeNetInterface(NetInterface niRef)
        throws DatabaseException
    {
        checkDeleted();

        netInterfaceMap.remove(niRef.getName());

        if (Objects.equals(activeStltConn.get(), niRef))
        {
            removeActiveSatelliteconnection();
        }
    }

    public void setOfflinePeer(ErrorReporter errorReporterRef)
    {
        peer = createOfflinePeer(errorReporterRef, false);
    }

    private Peer createOfflinePeer(ErrorReporter errorReporterRef, boolean createStltImplErrPeer)
    {
        checkDeleted();
        Peer localPear;
        if (!createStltImplErrPeer)
        {
            final String nodeNameStr = nodeName.displayValue;
            if (Node.Type.CONTROLLER.equals(nodeType.get()))
            {
                boolean isLocal = false;
                Set<String> allMyIps = LocalInetAddresses.LOCAL_ADDRESSES;
                for (Entry<NetInterfaceName, NetInterface> entry : netInterfaceMap.entrySet())
                {
                    String ipAddress = entry.getValue().getAddress().getAddress();
                    for (String inetAddress : allMyIps)
                    {
                        if (ipAddress.equals(inetAddress))
                        {
                            isLocal = true;
                            break;
                        }
                    }
                }
                localPear = new PeerController(errorReporterRef, nodeNameStr, this, isLocal);
            }
            else
            {
                localPear = new PeerOffline(errorReporterRef, nodeNameStr, this);
            }
        }
        else
        {
            localPear = new StltImplErrPeer();
        }
        return localPear;
    }

    public Iterator<NetInterface> iterateNetInterfaces()
    {
        checkDeleted();

        return netInterfaceMap.values().iterator();
    }

    public Stream<NetInterface> streamNetInterfaces()
    {
        checkDeleted();

        return netInterfaceMap.values().stream();
    }

    public @Nullable StorPool getStorPool(StorPoolName poolName)
    {
        checkDeleted();

        return storPoolMap.get(poolName);
    }

    public void addStorPool(StorPool pool)
    {
        checkDeleted();

        storPoolMap.put(pool.getName(), pool);
    }

    public void removeStorPool(StorPool pool)
    {
        checkDeleted();

        storPoolMap.remove(pool.getName());
    }

    public int getStorPoolCount()
    {
        checkDeleted();
        return storPoolMap.size();
    }

    public Iterator<StorPool> iterateStorPools()
    {
        checkDeleted();

        return storPoolMap.values().iterator();
    }

    public Stream<StorPool> streamStorPools()
    {
        checkDeleted();

        return storPoolMap.values().stream();
    }

    public void copyStorPoolMap(Map<? super StorPoolName, ? super StorPool> dstMap)
    {
        checkDeleted();
        dstMap.putAll(storPoolMap);
    }

    public Type setNodeType(Type newType)
        throws DatabaseException
    {
        checkDeleted();

        return nodeType.set(newType);
    }

    public Type getNodeType()
    {
        checkDeleted();

        return nodeType.get();
    }

    public DynamicNumberPool getTcpPortPool()
    {
        checkDeleted();
        return tcpPortPool;
    }

    public boolean hasNodeType(Type reqType)
    {
        checkDeleted();

        long reqFlags = reqType.getFlagValue();
        return (nodeType.get().getFlagValue() & reqFlags) == reqFlags;
    }

    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public Peer getPeer()
    {
        checkDeleted();
        return peer;
    }

    public void setPeer(Peer peerRef)
    {
        checkDeleted();
        peer = peerRef;
        reconnectAttemptCount += 1;
    }

    public long getReconnectAttemptCount()
    {
        return reconnectAttemptCount;
    }

    public @Nullable NetInterface getActiveStltConn()
    {
        checkDeleted();
        return activeStltConn.get();
    }

    public void setActiveStltConn(NetInterface satelliteConnectionRef)
        throws DatabaseException
    {
        checkDeleted();

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

    void removeActiveSatelliteconnection()
        throws DatabaseException
    {
        checkDeleted();
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

    public void setEvictionTimestamp(@Nullable Long timestamp)
    {
        checkDeleted();
        evictionTimstamp = timestamp;
    }

    public @Nullable Long getEvictionTimstamp()
    {
        checkDeleted();
        return evictionTimstamp;
    }

    public void markDeleted() throws DatabaseException
    {
        checkDeleted();
        getFlags().enableFlags(Flags.DELETE);
    }

    @Override
    public void delete()
        throws DatabaseException
    {
        if (!deleted.get())
        {

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
                nodeConn.delete();
            }

            // Shallow copy the collection because elements may be removed from it
            ArrayList<NetInterface> netIfs = new ArrayList<>(netInterfaceMap.values());
            for (NetInterface netIf : netIfs)
            {
                netIf.delete();
            }

            // Shallow copy the collection because elements may be removed from it
            ArrayList<StorPool> storPools = new ArrayList<>(storPoolMap.values());
            for (StorPool storPool : storPools)
            {
                storPool.delete();
            }

            nodeProps.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    public void markEvicted() throws DatabaseException
    {
        checkDeleted();
        getFlags().enableFlags(Flags.EVICTED);
    }

    public void unMarkEvicted() throws DatabaseException
    {
        checkDeleted();
        getFlags().disableFlags(Flags.EVICTED);
    }

    public boolean isEvicted()
    {
        checkDeleted();
        // We can't use isUnset here, because EVICTED contains DELETE
        return getFlags().isSet(Flags.EVICTED);
    }

    public NodePojo getApiData(@Nullable Long fullSyncId, @Nullable Long updateId)
    {
        checkDeleted();
        return getApiData(true, fullSyncId, updateId);
    }

    NodePojo getApiData(
        boolean includeOtherNode,
        @Nullable Long fullSyncId,
        @Nullable Long updateId
    )
    {
        checkDeleted();
        List<NetInterfaceApi> netInterfaces = new ArrayList<>();
        for (NetInterface ni : streamNetInterfaces().collect(toList()))
        {
            netInterfaces.add(ni.getApiData());
        }

        List<NodeConnPojo> nodeConns = new ArrayList<>();
        if (includeOtherNode)
        {
            /*
             * otherNode's node connection must not be included to prevent an endless recursion between the two nodes
             */
            for (NodeConnection nodeConn : nodeConnections.values())
            {
                nodeConns.add(nodeConn.getApiData(this, fullSyncId, updateId));
            }
        }

        Peer tmpPeer = getPeer();
        ExtToolsManager extToolsManager;
        ConnectionStatus connectionStatus;
        @Nullable ApiConsts.Platform platform;
        @Nullable String osVariant;
        if (!(tmpPeer instanceof StltImplErrPeer))
        {
            extToolsManager = tmpPeer.getExtToolsManager();
            connectionStatus = tmpPeer.getConnectionStatus();
            platform = tmpPeer.getPlatform();
            osVariant = tmpPeer.getOsVariant();
        }
        else
        {
            extToolsManager = new ExtToolsManager(); // no known supported tools
            connectionStatus = ApiConsts.ConnectionStatus.UNKNOWN;
            platform = null;
            osVariant = null;
        }

        return new NodePojo(
            getUuid(),
            getName().getDisplayName(),
            getNodeType().name(),
            getFlags().getFlagsBits(),
            netInterfaces,
            activeStltConn.get() != null ? activeStltConn.get().getApiData() : null,
            nodeConns,
            getProps().cloneMap(),
            connectionStatus,
            platform,
            osVariant,
            fullSyncId,
            updateId,
            extToolsManager.getSupportedLayers().stream()
                .map(deviceLayerKind -> deviceLayerKind.name())
                .collect(toList()),
            extToolsManager.getSupportedProviders().stream()
                .map(deviceProviderKind -> deviceProviderKind.name())
                .collect(toList()),
            extToolsManager.getUnsupportedLayersWithReasonsAsString(),
            extToolsManager.getUnsupportedProvidersWithReasonsAsString(),
            evictionTimstamp,
            reconnectAttemptCount
        );
    }

    public void setCryptoEntries(Collection<ProcCryptoEntry> procCryptoEntries) {
        checkDeleted();
        supportedCryptos.clear();
        supportedCryptos.addAll(procCryptoEntries);
    }

    public ArrayList<ProcCryptoEntry> getSupportedCryptos()
    {
        checkDeleted();
        return supportedCryptos;
    }

    @Override
    public String toStringImpl()
    {
        return "Node: '" + nodeName + "'";
    }

    public void registerInitialConnectSink(Object key, FluxSink<Boolean> fluxSinkRef)
    {
        checkDeleted();
        synchronized (initialConnectSinkMap)
        {
            initialConnectSinkMap.put(key, fluxSinkRef);
        }
    }

    public void connectionEstablished(@Nullable Object initialConnectSinkKeyRef)
    {
        checkDeleted();
        if (initialConnectSinkKeyRef != null)
        {
            @Nullable FluxSink<Boolean> initialConnectSink;
            synchronized (initialConnectSinkMap)
            {
                initialConnectSink = initialConnectSinkMap.remove(initialConnectSinkKeyRef);
            }
            if (initialConnectSink != null)
            {
                initialConnectSink.next(true);
                initialConnectSink.complete();
            }
        }
    }

    /**
     * cancels the initialConnectSink of the given key
     */
    public void removeInitialConnectSink(Object key)
    {
        checkDeleted();
        synchronized (initialConnectSinkMap)
        {
            FluxSink<Boolean> initialConnectSink = initialConnectSinkMap.remove(key);
            if (initialConnectSink != null)
            {
                // if not handled until now, we just pretend initial connect failed...
                initialConnectSink.next(false);
                initialConnectSink.complete();
            }
        }
    }

    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        DELETE(1L),
        EVICTED(DELETE.flagValue | 1L << 1),
        EVACUATE(1L << 2),
        QIGNORE(0x10000L),
        ;

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
                String[] split = StringUtils.split(string, ",");
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

    @SuppressWarnings("ImmutableEnumChecker") // list is wrapped with Collections.unmodifiableList
    public enum Type implements com.linbit.linstor.stateflags.Flags
    {
        CONTROLLER(
            1,
            Collections.emptyList(),
            false
        ),
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
                DeviceProviderKind.SPDK,
                DeviceProviderKind.EBS_INIT,
                DeviceProviderKind.STORAGE_SPACES,
                DeviceProviderKind.STORAGE_SPACES_THIN
            ),
            false
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
                DeviceProviderKind.SPDK,
                DeviceProviderKind.EBS_INIT,
                DeviceProviderKind.STORAGE_SPACES,
                DeviceProviderKind.STORAGE_SPACES_THIN
            ),
            false
        ),
        AUXILIARY(
            4,
            Collections.emptyList(),
            false
        ),

        // OPENFLEX_TARGET had flagValue 5

        REMOTE_SPDK(
            6,
            Arrays.asList(
                DeviceProviderKind.REMOTE_SPDK
            ),
            true
        ),
        EBS_TARGET(
            7,
            Arrays.asList(
                DeviceProviderKind.EBS_TARGET
            ),
            true
        );

        private final int flag;
        private final List<DeviceProviderKind> allowedKindClasses;
        private final boolean isSpecial;

        Type(int flagValue, List<DeviceProviderKind> allowedKindClassesRef, boolean isSpecialRef)
        {

            flag = flagValue;
            isSpecial = isSpecialRef;
            allowedKindClasses = Collections.unmodifiableList(allowedKindClassesRef);
        }

        @Override
        public long getFlagValue()
        {
            return flag;
        }

        public static @Nullable Type getByValue(long value)
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

        public static Type valueOfIgnoreCase(@Nullable String string, @Nullable Type defaultValue)
            throws IllegalArgumentException
        {
            Type ret = defaultValue;
            if (string != null)
            {
                ret = valueOf(string.toUpperCase());
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

        public boolean isSpecial()
        {
            return isSpecial;
        }
    }
}
