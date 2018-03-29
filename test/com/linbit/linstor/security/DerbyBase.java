package com.linbit.linstor.security;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import javax.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.testing.fieldbinder.Bind;
import com.google.inject.testing.fieldbinder.BoundFieldModule;
import com.linbit.GuiceConfigModule;
import com.linbit.InvalidNameException;
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
import com.linbit.linstor.ResourceDefinitionDataControllerFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDataFactory;
import com.linbit.linstor.StorPoolDefinitionDataFactory;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.VolumeConnectionDataFactory;
import com.linbit.linstor.VolumeDataFactory;
import com.linbit.linstor.VolumeDefinitionDataControllerFactory;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbcp.DbConnectionPoolModule;
import com.linbit.linstor.dbcp.TestDbConnectionPoolLoader;
import com.linbit.linstor.dbdrivers.ControllerDbModule;
import com.linbit.linstor.dbdrivers.DatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.testutils.TestCoreModule;
import com.linbit.linstor.transaction.AbsTransactionObject;
import com.linbit.linstor.transaction.ControllerTransactionMgr;
import com.linbit.linstor.transaction.ControllerTransactionMgrModule;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.inject.Named;
import javax.inject.Provider;

import java.lang.reflect.Field;
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

public abstract class DerbyBase implements DerbyTestConstants
{
    @Rule
    public TestName testMethodName = new TestName();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10); // 10 seconds max per method tested

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

    @Mock @Bind @Named(NumberPoolModule.MINOR_NUMBER_POOL)
    protected DynamicNumberPool minorNrPoolMock;

    @Mock @Bind @Named(NumberPoolModule.UNINITIALIZED_MINOR_NUMBER_POOL)
    protected DynamicNumberPool uninitializedMinorNrPoolMock;

    @Mock @Bind @Named(NumberPoolModule.TCP_PORT_POOL)
    protected DynamicNumberPool tcpPortPoolMock;

    @Mock @Bind @Named(NumberPoolModule.UNINITIALIZED_TCP_PORT_POOL)
    protected DynamicNumberPool uninitializedTcpPortPoolMock;

    @Inject private DbAccessor secureDbDriver;
    @Inject private DatabaseDriver persistenceDbDriver;
    @Inject private SecurityTestUtils securityTestUtils;
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
    @Inject protected VolumeDefinitionDataControllerFactory volumeDefinitionDataFactory;
    @Inject protected ResourceDefinitionDataControllerFactory resourceDefinitionDataFactory;
    @Inject protected NetInterfaceDataFactory netInterfaceDataFactory;

    @Inject protected LinStorScope testScope;
    @Inject protected TransactionObjectFactory transObjFactory;
    @Inject protected Provider<TransactionMgr> transMgrProvider;

    @BeforeClass
    public static void setUpBeforeClass()
        throws SQLException, InvalidNameException
    {
        if (dbConnPool == null)
        {
            errorReporter.logTrace("Performing DB initialization");

            TestDbConnectionPoolLoader dbConnectionPoolLoader = new TestDbConnectionPoolLoader();
            dbConnPool = dbConnectionPoolLoader.loadDbConnectionPool();

            dbConnPool.migrate(dbConnectionPoolLoader.getDbType());

            DbDerbyPersistence initializationSecureDbDriver = new DbDerbyPersistence();

            SecurityLevel.load(dbConnPool, initializationSecureDbDriver);
            Identity.load(dbConnPool, initializationSecureDbDriver);
            SecurityType.load(dbConnPool, initializationSecureDbDriver);
            Role.load(dbConnPool, initializationSecureDbDriver);
        }
    }

    protected void setUpAndEnterScope() throws Exception
    {
        setUpWithoutEnteringScope(new TestCoreModule());
        enterScope();
    }

    protected void setUpWithoutEnteringScope(Module additionalModule) throws Exception
    {
        con = getNewConnection();

        errorReporter.logTrace("Running cleanups for next method: %s", testMethodName.getMethodName());
        truncateTables();
        insertDefaults(con);
        errorReporter.logTrace("cleanups done, initializing: %s", testMethodName.getMethodName());
//        dbConnPool.returnConnection(con);

        MockitoAnnotations.initMocks(this);

        Injector injector = Guice.createInjector(
            new GuiceConfigModule(),
            new LoggingModule(errorReporter),
            new TestSecurityModule(SYS_CTX),
            new CoreModule(),
            new SharedDbConnectionPoolModule(),
            new ControllerDbModule(),
            new ControllerTransactionMgrModule(),
            new TestApiModule(),
            additionalModule,
            BoundFieldModule.of(this)
        );
        injector.injectMembers(this);

        TransactionMgr transMgr = new ControllerTransactionMgr(dbConnPool);
        testScope.enter();
        testScope.seed(TransactionMgr.class, transMgr);

        // make sure to seal the internal caches
        persistenceDbDriver.loadAll();

        transMgr.commit();
        testScope.exit();
        transMgr.returnConnection();

        clearCaches();
    }

    protected void enterScope() throws Exception
    {
        TransactionMgr transMgr = new ControllerTransactionMgr(dbConnPool);
        testScope.enter();
        testScope.seed(TransactionMgr.class, transMgr);
    }

    protected void clearCaches()
    {
        nodesMap.clear();
        rscDfnMap.clear();
        storPoolDfnMap.clear();
    }

    @After
    public void tearDown() throws Exception
    {
        try
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

            if (dbConnPool != null)
            {
                dbConnPool.closeAllThreadLocalConnections();
            }
        }
        finally
        {
            testScope.exit();
        }

        { // FIXME this is just a quick and dirty bugfix
            // find a better way to fix the static initialized-problem for these tests
            Field initializedField = AbsTransactionObject.class.getDeclaredField("initialized");
            initializedField.setAccessible(true);
            initializedField.set(null, false);
            initializedField.setAccessible(false);
        }
    }

    protected Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    protected Connection getNewConnection() throws SQLException
    {
        Connection connection = dbConnPool.getConnection();
        connection.setAutoCommit(false);
        connections.add(connection);
        return connection;
    }

    protected void commit() throws SQLException
    {
        transMgrProvider.get().commit();
    }

    protected void add(Statement stmt)
    {
        statements.add(stmt);
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
        con.commit();
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

    protected void testProps(
        String instanceName,
        Map<String, String> testMap
    )
        throws SQLException
    {
        TreeMap<String, String> map = new TreeMap<>(testMap);
        PreparedStatement stmt = getConnection().prepareStatement(SELECT_PROPS_BY_INSTANCE);
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

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertIdentity(TransactionMgr transMgr, IdentityName name) throws SQLException
    {
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(
            "INSERT INTO " + TBL_SEC_IDENTITIES +
            " (" + IDENTITY_NAME + ", " + IDENTITY_DSP_NAME + ") " +
            " VALUES (?, ?)"
        );
        stmt.setString(1, name.value);
        stmt.setString(2, name.displayValue);
        stmt.executeUpdate();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertSecType(TransactionMgr transMgr, SecTypeName name) throws SQLException
    {
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(
            "INSERT INTO " + TBL_SEC_TYPES +
            " (" + TYPE_NAME + ", " + TYPE_DSP_NAME + ") " +
            " VALUES (?, ?)"
        );
        stmt.setString(1, name.value);
        stmt.setString(2, name.displayValue);
        stmt.executeUpdate();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertRole(TransactionMgr transMgr, RoleName name, SecTypeName domain) throws SQLException
    {
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(
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

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertObjProt(
        TransactionMgr transMgr,
        String objPath,
        AccessContext accCtx
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(INSERT_SEC_OBJECT_PROTECTION);
        stmt.setString(1, objPath);
        stmt.setString(2, accCtx.subjectId.name.value);
        stmt.setString(3, accCtx.subjectRole.name.value);
        stmt.setString(4, accCtx.subjectDomain.name.value);
        stmt.executeUpdate();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertNode(
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

        PreparedStatement stmt = getConnection().prepareStatement(INSERT_NODES);
        stmt.setString(1, uuid.toString());
        stmt.setString(2, nodeName.value);
        stmt.setString(3, nodeName.displayValue);
        stmt.setLong(4, flags);
        stmt.setLong(5, typeMask);
        stmt.executeUpdate();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertNetInterface(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName nodeName,
        NetInterfaceName netName,
        String inetAddr,
        String transportType
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(INSERT_NODE_NET_INTERFACES);
        stmt.setString(1, uuid.toString());
        stmt.setString(2, nodeName.value);
        stmt.setString(3, netName.value);
        stmt.setString(4, netName.displayValue);
        stmt.setString(5, inetAddr);
        stmt.setString(6, transportType);
        stmt.executeUpdate();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertNodeCon(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName sourceNodeName,
        NodeName targetNodeName
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(INSERT_NODE_CONNECTIONS);
        stmt.setString(1, uuid.toString());
        stmt.setString(2, sourceNodeName.value);
        stmt.setString(3, targetNodeName.value);
        stmt.executeUpdate();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertResCon(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName sourceNodeName,
        NodeName targetNodeName,
        ResourceName resName
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(INSERT_RESOURCE_CONNECTIONS);
        stmt.setString(1, uuid.toString());
        stmt.setString(2, sourceNodeName.value);
        stmt.setString(3, targetNodeName.value);
        stmt.setString(4, resName.value);
        stmt.executeUpdate();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertVolCon(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName sourceNodeName,
        NodeName targetNodeName,
        ResourceName resName,
        VolumeNumber volDfnNr
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(INSERT_VOLUME_CONNECTIONS);
        stmt.setString(1, uuid.toString());
        stmt.setString(2, sourceNodeName.value);
        stmt.setString(3, targetNodeName.value);
        stmt.setString(4, resName.value);
        stmt.setInt(5, volDfnNr.value);
        stmt.executeUpdate();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertResDfn(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        ResourceName resName,
        RscDfnFlags... flags
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(INSERT_RESOURCE_DEFINITIONS);
        stmt.setString(1, uuid.toString());
        stmt.setString(2, resName.value);
        stmt.setString(3, resName.displayValue);
        stmt.setLong(4, StateFlagsBits.getMask(flags));
        stmt.executeUpdate();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertRes(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName nodeName,
        ResourceName resName,
        NodeId nodeId,
        Resource.RscFlags... resFlags
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(INSERT_RESOURCES);
        stmt.setString(1, uuid.toString());
        stmt.setString(2, nodeName.value);
        stmt.setString(3, resName.value);
        stmt.setInt(4, nodeId.value);
        stmt.setLong(5, StateFlagsBits.getMask(resFlags));
        stmt.executeUpdate();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertVolDfn(
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
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(INSERT_VOLUME_DEFINITIONS);
        stmt.setString(1, uuid.toString());
        stmt.setString(2, resName.value);
        stmt.setInt(3, volId.value);
        stmt.setLong(4, volSize);
        stmt.setInt(5, minorNr);
        stmt.setLong(6, flags);
        stmt.executeUpdate();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertVol(
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
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(INSERT_VOLUMES);
        stmt.setString(1, uuid.toString());
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

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertStorPoolDfn(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        StorPoolName poolName
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(INSERT_STOR_POOL_DEFINITIONS);
        stmt.setString(1, uuid.toString());
        stmt.setString(2, poolName.value);
        stmt.setString(3, poolName.displayValue);
        stmt.executeUpdate();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertStorPool(
        TransactionMgr transMgr,
        java.util.UUID uuid,
        NodeName nodeName,
        StorPoolName poolName,
        String driver
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.getConnection().prepareStatement(INSERT_NODE_STOR_POOL);
        stmt.setString(1, uuid.toString());
        stmt.setString(2, nodeName.value);
        stmt.setString(3, poolName.value);
        stmt.setString(4, driver);
        stmt.executeUpdate();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void insertProp(
        String instance,
        String key,
        String value
    )
        throws SQLException
    {
        PreparedStatement stmt = getConnection().prepareStatement(INSERT_PROPS_CONTAINERS);
        stmt.setString(1, instance.toUpperCase());
        stmt.setString(2, key);
        stmt.setString(3, value);
        stmt.executeUpdate();
        stmt.close();
    }

    protected ObjectProtection createTestObjectProtection(
        AccessContext accCtx,
        String objPath
    )
    {
        return securityTestUtils.createObjectProtection(accCtx, objPath);
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
