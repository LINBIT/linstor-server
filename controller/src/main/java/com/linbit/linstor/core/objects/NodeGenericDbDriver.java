package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class NodeGenericDbDriver implements NodeDatabaseDriver
{
    private static final String TBL_NODE = DbConstants.TBL_NODES;

    private static final String NODE_UUID = DbConstants.UUID;
    private static final String NODE_NAME = DbConstants.NODE_NAME;
    private static final String NODE_DSP_NAME = DbConstants.NODE_DSP_NAME;
    private static final String NODE_FLAGS = DbConstants.NODE_FLAGS;
    private static final String NODE_TYPE = DbConstants.NODE_TYPE;

    private static final String NODE_SELECT_ALL =
        " SELECT " + NODE_UUID + ", " + NODE_DSP_NAME + ", " + NODE_TYPE + ", " + NODE_FLAGS +
        " FROM " + TBL_NODE;
    private static final String NODE_INSERT =
        " INSERT INTO " + TBL_NODE +
        " (" +
            NODE_UUID + ", " + NODE_NAME + ", " + NODE_DSP_NAME + ", " +
            NODE_FLAGS + ", " + NODE_TYPE +
        ") VALUES (?, ?, ?, ?, ?)";
    private static final String NODE_UPDATE_FLAGS =
        " UPDATE " + TBL_NODE +
        " SET "   + NODE_FLAGS + " = ? " +
        " WHERE " + NODE_NAME +  " = ?";
    private static final String NODE_UPDATE_TYPE =
        " UPDATE " + TBL_NODE +
        " SET "   + NODE_TYPE + " = ? " +
        " WHERE " + NODE_NAME + " = ?";
    private static final String NODE_DELETE =
        " DELETE FROM " + TBL_NODE +
        " WHERE " + NODE_NAME + " = ?";


    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final StateFlagsPersistence<Node> flagDriver;
    private final SingleColumnDatabaseDriver<Node, Node.Type> typeDriver;

    private final ObjectProtectionDatabaseDriver objProtDriver;
    private final PropsContainerFactory propsContainerFactory;

    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    @Inject
    public NodeGenericDbDriver(
        @SystemContext AccessContext privCtx,
        ErrorReporter errorReporterRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        dbCtx = privCtx;
        errorReporter = errorReporterRef;
        objProtDriver = objProtDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        flagDriver = new NodeFlagPersistence();
        typeDriver = new NodeTypeDriver();

    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(Node node) throws DatabaseException
    {
        errorReporter.logTrace("Creating Node %s", getId(node));
        try (PreparedStatement stmt = getConnection().prepareStatement(NODE_INSERT))
        {
            stmt.setString(1, node.getUuid().toString());
            stmt.setString(2, node.getName().value);
            stmt.setString(3, node.getName().displayValue);
            stmt.setLong(4, node.getFlags().getFlagsBits(dbCtx));
            stmt.setLong(5, node.getNodeType(dbCtx).getFlagValue());
            stmt.executeUpdate();

            errorReporter.logTrace("Node created %s", getId(node));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
        }
    }

    public Map<Node, Node.InitMaps> loadAll() throws DatabaseException
    {
        errorReporter.logTrace("Loading all Nodes");
        Map<Node, Node.InitMaps> loadedNodesMap = new TreeMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(NODE_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    Pair<Node, Node.InitMaps> pair = restoreNode(resultSet);
                    loadedNodesMap.put(
                        pair.objA,
                        pair.objB
                    );
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("Loaded %d Nodes", loadedNodesMap.size());
        return loadedNodesMap;
    }

    private Pair<Node, Node.InitMaps> restoreNode(ResultSet resultSet)
        throws DatabaseException, ImplementationError
    {
        Pair<Node, Node.InitMaps> retPair = new Pair<>();
        Node node;
        NodeName nodeName;

        try
        {
            try
            {
                nodeName = new NodeName(resultSet.getString(NODE_DSP_NAME));
            }
            catch (InvalidNameException invalidNameExc)
            {
                throw new LinStorDBRuntimeException(
                    String.format(
                        "The display name of a stored Node could not be restored" +
                        "(invalid display NodeName=%s)",
                        resultSet.getString(NODE_DSP_NAME)
                        ),
                    invalidNameExc
                );
            }

            ObjectProtection objProt = getObjectProtection(nodeName);

            final Map<ResourceName, Resource> rscMap = new TreeMap<>();
            final Map<SnapshotDefinition.Key, Snapshot> snapshotMap = new TreeMap<>();
            final Map<NetInterfaceName, NetInterface> netIfMap = new TreeMap<>();
            final Map<StorPoolName, StorPool> storPoolMap = new TreeMap<>();
            final Map<NodeName, NodeConnection> nodeConnMap = new TreeMap<>();

            node = new Node(
                java.util.UUID.fromString(resultSet.getString(NODE_UUID)),
                objProt,
                nodeName,
                Node.Type.getByValue(resultSet.getLong(NODE_TYPE)),
                resultSet.getLong(NODE_FLAGS),
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

            retPair.objA = node;
            retPair.objB = new NodeInitMaps(
                rscMap,
                snapshotMap,
                netIfMap,
                storPoolMap,
                nodeConnMap
            );
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }

        errorReporter.logTrace("Node loaded from DB %s", getId(node));
        return retPair;
    }

    private ObjectProtection getObjectProtection(NodeName nodeName) throws DatabaseException
    {
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPath(nodeName),
            false // no need to log a warning, as we would fail then anyways
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                "Node's DB entry exists, but is missing an entry in ObjProt table! " + getId(nodeName),
                null
            );
        }
        return objProt;
    }

    @Override
    public void delete(Node node) throws DatabaseException
    {
        errorReporter.logTrace("Deleting node %s", getId(node));
        try (PreparedStatement stmt = getConnection().prepareStatement(NODE_DELETE))
        {
            stmt.setString(1, node.getName().value);

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("Node deleted %s", getId(node));
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    @Override
    public StateFlagsPersistence<Node> getStateFlagPersistence()
    {
        return flagDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Node, Node.Type> getNodeTypeDriver()
    {
        return typeDriver;
    }

    private String getId(Node node)
    {
        return getId(node.getName().displayValue);
    }

    private String getId(NodeName nodeName)
    {
        return getId(nodeName.displayValue);
    }

    private String getId(String nodeName)
    {
        return "(NodeName=" + nodeName + ")";
    }

    private class NodeFlagPersistence implements StateFlagsPersistence<Node>
    {
        @Override
        public void persist(Node node, long oldFlagBits, long newFlagBits) throws DatabaseException
        {
            String fromFlags = StringUtils.join(
                FlagsHelper.toStringList(
                    Node.Flags.class,
                    oldFlagBits
                ),
                ", "
            );
            String toFlags = StringUtils.join(
                FlagsHelper.toStringList(
                    Node.Flags.class,
                    newFlagBits
                ),
                ", "
            );

            errorReporter.logTrace(
                "Updating Node's flags from [%s] to [%s] %s",
                fromFlags,
                toFlags,
                getId(node)
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(NODE_UPDATE_FLAGS))
            {
                stmt.setLong(1, newFlagBits);
                stmt.setString(2, node.getName().value);

                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "Node's flags updated from [%s] to [%s] %s",
                fromFlags,
                toFlags,
                getId(node)
            );
        }
    }

    private class NodeTypeDriver implements SingleColumnDatabaseDriver<Node, Node.Type>
    {
        @Override
        public void update(Node parent, Node.Type element) throws DatabaseException
        {
            try
            {
                errorReporter.logTrace("Updating Node's NodeType from [%s] to [%s] %s",
                    parent.getNodeType(dbCtx).name(),
                    element.name(),
                    getId(parent)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(NODE_UPDATE_TYPE))
                {
                    stmt.setLong(1, element.getFlagValue());
                    stmt.setString(2, parent.getName().value);

                    stmt.executeUpdate();
                }
                errorReporter.logTrace("Node's NodeType updated from [%s] to [%s] %s",
                    parent.getNodeType(dbCtx).name(),
                    element.name(),
                    getId(parent)
                );
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DatabaseLoader.handleAccessDeniedException(accDeniedExc);
            }
        }
    }

    private class NodeInitMaps implements Node.InitMaps
    {
        private final Map<ResourceName, Resource> rscMap;
        private final Map<SnapshotDefinition.Key, Snapshot> snapshotMap;
        private final Map<NetInterfaceName, NetInterface> netIfMap;
        private final Map<StorPoolName, StorPool> storPoolMap;
        private final Map<NodeName, NodeConnection> nodeConnMap;

        NodeInitMaps(
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
