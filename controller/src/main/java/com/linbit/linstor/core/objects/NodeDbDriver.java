package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.dbdrivers.AbsProtectedDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.NodeCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Nodes.NODE_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Nodes.NODE_FLAGS;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Nodes.NODE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Nodes.NODE_TYPE;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Nodes.UUID;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;

@Singleton
public final class NodeDbDriver extends AbsProtectedDatabaseDriver<Node, Node.InitMaps, Void>
    implements NodeCtrlDatabaseDriver
{
    protected final PropsContainerFactory propsContainerFactory;
    protected final TransactionObjectFactory transObjFactory;
    protected final Provider<? extends TransactionMgr> transMgrProvider;

    protected final StateFlagsPersistence<Node> flagsDriver;
    protected final SingleColumnDatabaseDriver<Node, Node.Type> nodeTypeDriver;
    private ReadOnlyProps ctrlConf;

    @Inject
    public NodeDbDriver(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext dbCtxRef,
        @Named(LinStor.CONTROLLER_PROPS) ReadOnlyProps ctrlConfRef,
        DbEngine dbEngine,
        Provider<TransactionMgr> transMgrProviderRef,
        ObjectProtectionFactory objProtFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.NODES, dbEngine, objProtFactoryRef);
        ctrlConf = ctrlConfRef;
        transMgrProvider = transMgrProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;

        setColumnSetter(UUID, node -> node.getUuid().toString());
        setColumnSetter(NODE_NAME, node -> node.getName().value);
        setColumnSetter(NODE_DSP_NAME, node -> node.getName().displayValue);
        setColumnSetter(NODE_FLAGS, node -> node.getFlags().getFlagsBits(dbCtxRef));
        switch (getDbType())
        {
            case SQL: // fall-through
                setColumnSetter(NODE_TYPE, node -> node.getNodeType(dbCtxRef).getFlagValue());
                break;
            case K8S_CRD:
                // TODO make a DB migration changing the NODE_TYPE from INTEGER to BIGINT
                setColumnSetter(NODE_TYPE, node -> (int) node.getNodeType(dbCtxRef).getFlagValue());
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        flagsDriver = generateFlagDriver(NODE_FLAGS, Node.Flags.class);
        nodeTypeDriver = generateSingleColumnDriver(
            NODE_TYPE,
            node -> node.getNodeType(dbCtxRef).toString(),
            Node.Type::getFlagValue
        );
    }


    @Override
    public StateFlagsPersistence<Node> getStateFlagPersistence()
    {
        return flagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Node, Node.Type> getNodeTypeDriver()
    {
        return nodeTypeDriver;
    }

    @Override
    protected String getId(Node dataRef)
    {
        return "Node(" + dataRef.getName().displayValue + ")";
    }

    @Override
    protected Pair<Node, Node.InitMaps> load(RawParameters raw, Void ignored)
        throws DatabaseException, InvalidNameException, AccessDeniedException
    {
        final Map<ResourceName, Resource> rscMap = new TreeMap<>();
        final Map<SnapshotDefinition.Key, Snapshot> snapshotMap = new TreeMap<>();
        final Map<NetInterfaceName, NetInterface> netIfMap = new TreeMap<>();
        final Map<StorPoolName, StorPool> storPoolMap = new TreeMap<>();
        final Map<NodeName, NodeConnection> nodeConnMap = new TreeMap<>();

        final NodeName nodeName = raw.build(NODE_DSP_NAME, NodeName::new);
        final Node.Type nodeType;
        final long flags;
        switch (getDbType())
        {
            case SQL:
                nodeType = Node.Type.getByValue(raw.<Integer>get(NODE_TYPE).longValue());
                flags = raw.get(NODE_FLAGS);
                break;
            case K8S_CRD:
                nodeType = Node.Type.getByValue(raw.<Integer>get(NODE_TYPE));
                flags = raw.get(NODE_FLAGS);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + getDbType());
        }

        Node node = new Node(
            raw.build(UUID, java.util.UUID::fromString),
            getObjectProtection(ObjectProtection.buildPath(nodeName)),
            nodeName,
            nodeType,
            flags,
            ctrlConf,
            errorReporter,
            this,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            rscMap,
            snapshotMap,
            netIfMap,
            storPoolMap,
            nodeConnMap
        );
        node.setOfflinePeer(errorReporter, dbCtx);

        return new Pair<>(
            node,
            new InitMapsImpl(
                rscMap,
                snapshotMap,
                netIfMap,
                storPoolMap,
                nodeConnMap
            )
        );
    }

    private class InitMapsImpl implements Node.InitMaps
    {
        private final Map<ResourceName, Resource> rscMap;
        private final Map<SnapshotDefinition.Key, Snapshot> snapshotMap;
        private final Map<NetInterfaceName, NetInterface> netIfMap;
        private final Map<StorPoolName, StorPool> storPoolMap;
        private final Map<NodeName, NodeConnection> nodeConnMap;

        private InitMapsImpl(
            Map<ResourceName, Resource> rscMapRef,
            Map<SnapshotDefinition.Key, Snapshot> snapshotMapRef,
            Map<NetInterfaceName, NetInterface> netIfMapRef,
            Map<StorPoolName, StorPool> storPoolMapRef,
            Map<NodeName, NodeConnection> nodeConnMapRef
        )
        {
            rscMap = rscMapRef;
            snapshotMap = snapshotMapRef;
            netIfMap = netIfMapRef;
            storPoolMap = storPoolMapRef;
            nodeConnMap = nodeConnMapRef;
        }

        @Override
        public Map<ResourceName, Resource> getRscMap()
        {
            return rscMap;
        }

        @Override
        public Map<SnapshotDefinition.Key, Snapshot> getSnapshotMap()
        {
            return snapshotMap;
        }

        @Override
        public Map<NetInterfaceName, NetInterface> getNetIfMap()
        {
            return netIfMap;
        }

        @Override
        public Map<StorPoolName, StorPool> getStorPoolMap()
        {
            return storPoolMap;
        }

        @Override
        public Map<NodeName, NodeConnection> getNodeConnMap()
        {
            return nodeConnMap;
        }
    }
}
