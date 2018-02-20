package com.linbit.linstor.security;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.linbit.GuiceConfigModule;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.NetInterfaceDataFactory;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.NodeConnectionDataFactory;
import com.linbit.linstor.NodeDataControllerFactory;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnectionDataFactory;
import com.linbit.linstor.ResourceDataFactory;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinitionDataFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDataFactory;
import com.linbit.linstor.StorPoolDefinitionDataFactory;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.VolumeConnectionDataFactory;
import com.linbit.linstor.VolumeDataFactory;
import com.linbit.linstor.VolumeDefinitionDataFactory;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbcp.TestDbConnectionPoolLoader;
import com.linbit.linstor.dbdrivers.ControllerDbModule;
import com.linbit.linstor.dbdrivers.DatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.utils.UuidUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class DerbyBase implements DerbyTestConstants
{
    @Rule
    public TestName testMethodName = new TestName();

    private static final String SELECT_PROPS_BY_INSTANCE =
        " SELECT " + PROPS_INSTANCE + ", " + PROP_KEY + ", " + PROP_VALUE +
        " FROM " + TBL_PROPS_CONTAINERS +
        " WHERE " + PROPS_INSTANCE + " = ? " +
        " ORDER BY " + PROP_KEY;

    protected static ErrorReporter errorReporter =
        new StdErrorReporter("TESTS", "");

    protected static final AccessContext SYS_CTX = DummySecurityInitializer.getSystemAccessContext();
    protected static final AccessContext PUBLIC_CTX = DummySecurityInitializer.getPublicAccessContext();

    // This connection pool is shared between the tests
    protected static DbConnectionPool dbConnPool;

    private List<Statement> statements = new ArrayList<>();
    private Connection con;
    private List<Connection> connections = new ArrayList<>();

    @Inject private DbAccessor secureDbDriver;
    @Inject private DatabaseDriver persistenceDbDriver;
    @Inject protected CoreModule.NodesMap nodesMap;
    @Inject protected CoreModule.ResourceDefinitionMap rscDfnMap;
    @Inject protected CoreModule.StorPoolDefinitionMap storPoolDfnMap;

    @Inject protected ObjectProtectionFactory objectProtectionFactory;
    @Inject protected PropsContainerFactory propsContainerFactory;
    @Inject protected NodeDataControllerFactory nodeDataFactory;
    @Inject protected ResourceConnectionDataFactory resourceConnectionDataFactory;
    @Inject protected ResourceDataFactory resourceDataFactory;
    @Inject protected StorPoolDefinitionDataFactory storPoolDefinitionDataFactory;
    @Inject protected VolumeConnectionDataFactory volumeConnectionDataFactory;
    @Inject protected NodeConnectionDataFactory nodeConnectionDataFactory;
    @Inject protected StorPoolDataFactory storPoolDataFactory;
    @Inject protected VolumeDataFactory volumeDataFactory;
    @Inject protected VolumeDefinitionDataFactory volumeDefinitionDataFactory;
    @Inject protected ResourceDefinitionDataFactory resourceDefinitionDataFactory;
    @Inject protected NetInterfaceDataFactory netInterfaceDataFactory;

    @BeforeClass
    public static void setUpBeforeClass()
        throws SQLException, InvalidNameException
    {
        if (dbConnPool == null)
        {
            errorReporter.logTrace("Performing DB initialization");

            dbConnPool = new TestDbConnectionPoolLoader().loadDbConnectionPool();

            Connection connection = dbConnPool.getConnection();
            try
            {
                createTables(connection);
                insertDefaults(connection);
            }
            finally
            {
                dbConnPool.returnConnection(connection);
            }

            DbDerbyPersistence initializationSecureDbDriver = new DbDerbyPersistence(SYS_CTX, errorReporter);

            Identity.load(dbConnPool, initializationSecureDbDriver);
            SecurityType.load(dbConnPool, initializationSecureDbDriver);
            Role.load(dbConnPool, initializationSecureDbDriver);
        }
    }

    @Before
    public void setUp() throws Exception
    {
        setUp(Modules.EMPTY_MODULE);
    }

    public void setUp(Module additionalModule) throws Exception
    {
        con = getConnection();

        errorReporter.logTrace("Running cleanups for next method: %s", testMethodName.getMethodName());
        truncateTables();
        insertDefaults(con);
        errorReporter.logTrace("cleanups done, initializing: %s", testMethodName.getMethodName());

        Injector injector = Guice.createInjector(
            new GuiceConfigModule(),
            new LoggingModule(errorReporter),
            new TestSecurityModule(SYS_CTX),
            new CoreModule(),
            new SharedDbConnectionPoolModule(),
            new ControllerDbModule(),
            additionalModule
        );
        injector.injectMembers(this);

        Connection tmpCon = dbConnPool.getConnection();

        TransactionMgr transMgr = new TransactionMgr(tmpCon);
        // make sure to seal the internal caches
        persistenceDbDriver.loadAll(transMgr);

        transMgr.commit();
        dbConnPool.returnConnection(transMgr);

        clearCaches();
    }

    protected void clearCaches()
    {
        nodesMap.clear();
        rscDfnMap.clear();
        storPoolDfnMap.clear();
    }

    protected void satelliteMode()
    {
        // TODO
//        CoreUtils.satelliteMode(SYS_CTX, nodesMap, rscDfnMap, storPoolDfnMap);
    }

    @After
    public void tearDown() throws Exception
    {
        for (Statement statement : statements)
        {
            statement.close();
        }
        for (Connection connection : connections)
        {
            dbConnPool.returnConnection(connection);
        }
        connections.clear();

        if (dbConnPool != null && dbConnPool.closeAllThreadLocalConnections())
        {
            fail("Unclosed database connection");
        }
    }

    protected Connection getConnection() throws SQLException
    {
        Connection connection = dbConnPool.getConnection();
        connection.setAutoCommit(false);
        connections.add(connection);
        return connection;
    }

    protected void add(Statement stmt)
    {
        statements.add(stmt);
    }

    private static void createTables(Connection connection) throws SQLException
    {
        for (int idx = 0; idx < CREATE_TABLES.length; ++idx)
        {
            createTable(connection, true, idx);
        }
        connection.commit();
    }

    private static void insertDefaults(Connection connection) throws SQLException
    {
        for (String insert : INSERT_DEFAULT_VALUES)
        {
            try (PreparedStatement stmt = connection.prepareStatement(insert))
            {
                stmt.executeUpdate();
            }
        }
        connection.commit();
    }

//    private static void dropTables() throws SQLException
//    {
//        for (int idx = 0; idx < DROP_TABLES.length; ++idx)
//        {
//            dropTable(con, idx);
//        }
//    }

    private void truncateTables() throws SQLException
    {
        for (String sql : TRUNCATE_TABLES)
        {
            PreparedStatement stmt = con.prepareStatement(sql);
            stmt.executeUpdate();
            stmt.close();
        }
    }

    private static void createTable(Connection connection, boolean dropIfExists, int idx) throws SQLException
    {
        try
        {
//            System.out.print("creating... " + CREATE_TABLES[idx]);
            try (PreparedStatement stmt = connection.prepareStatement(CREATE_TABLES[idx]))
            {
                stmt.executeUpdate();
//                System.out.println("... done");
            }
        }
        catch (SQLException sqlExc)
        {
            String sqlState = sqlExc.getSQLState();
            if ("X0Y32".equals(sqlState)) // table already exists
            {
                if (dropIfExists)
                {
//                    System.out.print("exists, ");
                    dropTable(connection, DROP_TABLES.length - 1 - idx);
                    createTable(connection, false, idx);
                }
                else
                {
                    System.out.println(CREATE_TABLES[idx]);
                    throw sqlExc;
                }
            }
            else
            {
                System.out.println(CREATE_TABLES[idx]);
                throw sqlExc;
            }
        }
        connection.commit();
    }

    private static void dropTable(Connection connection, int idx) throws SQLException
    {
        try (PreparedStatement stmt = connection.prepareStatement(DROP_TABLES[idx]))
        {
//            System.out.print("dropping... " + DROP_TABLES[idx]);
            stmt.executeUpdate();
//            System.out.println("... done");
        }
        catch (SQLException sqlExc)
        {
            if ("42Y55".equals(sqlExc.getSQLState()))
            {
                // table does not exists.... yay - ignore
            }
            else
            {
                System.out.println(DROP_TABLES[idx]);
                throw sqlExc;
            }
        }
        connection.commit();
    }

    protected String debugGetAllProsContent() throws SQLException
    {
        Connection connection = getConnection();
        PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + TBL_PROPS_CONTAINERS);
        ResultSet allContent = stmt.executeQuery();
        StringBuilder sb = new StringBuilder();
        while (allContent.next())
        {
            sb.append(allContent.getString(1)).append(": ")
                .append(allContent.getString(2)).append(" = ")
                .append(allContent.getString(3)).append("\n");
        }
        allContent.close();
        stmt.close();
        connection.close();
        return sb.toString();
    }

    protected static java.util.UUID randomUUID()
    {
        return java.util.UUID.randomUUID();
    }

    protected static void testProps(
        TransactionMgr transMgr,
        String instanceName,
        Map<String, String> testMap
    )
        throws SQLException
    {
        TreeMap<String, String> map = new TreeMap<>(testMap);
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_PROPS_BY_INSTANCE);
        stmt.setString(1, instanceName.toUpperCase());
        ResultSet resultSet = stmt.executeQuery();

        while (resultSet.next())
        {
            String key = resultSet.getString(PROP_KEY);
            String value = resultSet.getString(PROP_VALUE);

            assertEquals(map.remove(key), value);
        }
        assertTrue(map.isEmpty());

        resultSet.close();
        stmt.close();
    }

    public static void insertIdentity(TransactionMgr transMgr, IdentityName name) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(
            "INSERT INTO " + TBL_SEC_IDENTITIES +
            " (" + IDENTITY_NAME + ", " + IDENTITY_DSP_NAME + ") " +
            " VALUES (?, ?)"
        );
        stmt.setString(1, name.value);
        stmt.setString(2, name.displayValue);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertSecType(TransactionMgr transMgr, SecTypeName name) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(
            "INSERT INTO " + TBL_SEC_TYPES +
            " (" + TYPE_NAME + ", " + TYPE_DSP_NAME + ") " +
            " VALUES (?, ?)"
        );
        stmt.setString(1, name.value);
        stmt.setString(2, name.displayValue);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertRole(TransactionMgr transMgr, RoleName name, SecTypeName domain) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(
            "INSERT INTO " + TBL_SEC_ROLES +
            " (" + ROLE_NAME + ", " + ROLE_DSP_NAME + ", " + DOMAIN_NAME + ") " +
            " VALUES (?, ?, ?)"
        );
        stmt.setString(1, name.value);
        stmt.setString(2, name.displayValue);
        stmt.setString(3, domain.value);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertObjProt(
        TransactionMgr transMgr,
        String objPath,
        AccessContext accCtx
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT_SEC_OBJECT_PROTECTION);
        stmt.setString(1, objPath);
        stmt.setString(2, accCtx.subjectId.name.value);
        stmt.setString(3, accCtx.subjectRole.name.value);
        stmt.setString(4, accCtx.subjectDomain.name.value);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertNode(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName nodeName,
        long flags,
        NodeType... types
    )
        throws SQLException
    {
        long typeMask = 0;
        for (NodeType type : types)
        {
            typeMask |= type.getFlagValue();
        }

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT_NODES);
        stmt.setBytes(1, UuidUtils.asByteArray(uuid));
        stmt.setString(2, nodeName.value);
        stmt.setString(3, nodeName.displayValue);
        stmt.setLong(4, flags);
        stmt.setLong(5, typeMask);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertNetInterface(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName nodeName,
        NetInterfaceName netName,
        String inetAddr,
        String transportType
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT_NODE_NET_INTERFACES);
        stmt.setBytes(1, UuidUtils.asByteArray(uuid));
        stmt.setString(2, nodeName.value);
        stmt.setString(3, netName.value);
        stmt.setString(4, netName.displayValue);
        stmt.setString(5, inetAddr);
        stmt.setString(6, transportType);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertNodeCon(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName sourceNodeName,
        NodeName targetNodeName
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT_NODE_CONNECTIONS);
        stmt.setBytes(1, UuidUtils.asByteArray(uuid));
        stmt.setString(2, sourceNodeName.value);
        stmt.setString(3, targetNodeName.value);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertResCon(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName sourceNodeName,
        NodeName targetNodeName,
        ResourceName resName
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT_RESOURCE_CONNECTIONS);
        stmt.setBytes(1, UuidUtils.asByteArray(uuid));
        stmt.setString(2, sourceNodeName.value);
        stmt.setString(3, targetNodeName.value);
        stmt.setString(4, resName.value);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertVolCon(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName sourceNodeName,
        NodeName targetNodeName,
        ResourceName resName,
        VolumeNumber volDfnNr
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT_VOLUME_CONNECTIONS);
        stmt.setBytes(1, UuidUtils.asByteArray(uuid));
        stmt.setString(2, sourceNodeName.value);
        stmt.setString(3, targetNodeName.value);
        stmt.setString(4, resName.value);
        stmt.setInt(5, volDfnNr.value);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertResDfn(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        ResourceName resName,
        RscDfnFlags... flags
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT_RESOURCE_DEFINITIONS);
        stmt.setBytes(1, UuidUtils.asByteArray(uuid));
        stmt.setString(2, resName.value);
        stmt.setString(3, resName.displayValue);
        stmt.setLong(4, StateFlagsBits.getMask(flags));
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertRes(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName nodeName,
        ResourceName resName,
        NodeId nodeId,
        Resource.RscFlags... resFlags
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT_RESOURCES);
        stmt.setBytes(1, UuidUtils.asByteArray(uuid));
        stmt.setString(2, nodeName.value);
        stmt.setString(3, resName.value);
        stmt.setInt(4, nodeId.value);
        stmt.setLong(5, StateFlagsBits.getMask(resFlags));
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertVolDfn(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        ResourceName resName,
        VolumeNumber volId,
        long volSize,
        int minorNr,
        long flags
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT_VOLUME_DEFINITIONS);
        stmt.setBytes(1, UuidUtils.asByteArray(uuid));
        stmt.setString(2, resName.value);
        stmt.setInt(3, volId.value);
        stmt.setLong(4, volSize);
        stmt.setInt(5, minorNr);
        stmt.setLong(6, flags);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertVol(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName nodeName,
        ResourceName resName,
        VolumeNumber volNr,
        StorPoolName storPoolName,
        String blockDev,
        String metaDisk,
        VlmFlags... flags
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT_VOLUMES);
        stmt.setBytes(1, UuidUtils.asByteArray(uuid));
        stmt.setString(2, nodeName.value);
        stmt.setString(3, resName.value);
        stmt.setInt(4, volNr.value);
        stmt.setString(5, storPoolName.value);
        stmt.setString(6, blockDev);
        stmt.setString(7, metaDisk);
        stmt.setLong(8, StateFlagsBits.getMask(flags));
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertStorPoolDfn(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        StorPoolName poolName
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT_STOR_POOL_DEFINITIONS);
        stmt.setBytes(1, UuidUtils.asByteArray(uuid));
        stmt.setString(2, poolName.value);
        stmt.setString(3, poolName.displayValue);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertStorPool(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName nodeName,
        StorPoolName poolName,
        String driver
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT_NODE_STOR_POOL);
        stmt.setBytes(1, UuidUtils.asByteArray(uuid));
        stmt.setString(2, nodeName.value);
        stmt.setString(3, poolName.value);
        stmt.setString(4, driver);
        stmt.executeUpdate();
        stmt.close();
    }

    public static void insertProp(
        TransactionMgr transMgr,
        String instance,
        String key,
        String value
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(INSERT_PROPS_CONTAINERS);
        stmt.setString(1, instance.toUpperCase());
        stmt.setString(2, key);
        stmt.setString(3, value);
        stmt.executeUpdate();
        stmt.close();
    }

    private class SharedDbConnectionPoolModule extends AbstractModule
    {
        @Override
        protected void configure()
        {
            bind(DbConnectionPool.class).toInstance(dbConnPool);

            bind(ControllerDatabase.class).to(DbConnectionPool.class);
        }
    }
}
