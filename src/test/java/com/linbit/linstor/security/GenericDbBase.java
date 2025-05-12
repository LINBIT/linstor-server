package com.linbit.linstor.security;

import com.linbit.GuiceConfigModule;
import com.linbit.InvalidNameException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerLinstorModule;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ErrorReporterContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.ApiTestModule;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DbDataInitializer;
import com.linbit.linstor.core.SatelliteConnectorImpl;
import com.linbit.linstor.core.SeedDefaultPeerRule;
import com.linbit.linstor.core.apicallhandler.ApiCallHandlerModule;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.core.cfg.CtrlConfigModule;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.core.objects.FreeSpaceMgrControllerFactory;
import com.linbit.linstor.core.objects.NetInterfaceFactory;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnectionFactory;
import com.linbit.linstor.core.objects.NodeControllerFactory;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnectionControllerFactory;
import com.linbit.linstor.core.objects.ResourceControllerFactory;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinitionControllerFactory;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.ResourceGroupControllerFactory;
import com.linbit.linstor.core.objects.ResourceGroupDbDriver;
import com.linbit.linstor.core.objects.SnapshotControllerFactory;
import com.linbit.linstor.core.objects.SnapshotDefinitionControllerFactory;
import com.linbit.linstor.core.objects.SnapshotVolumeControllerFactory;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinitionControllerFactory;
import com.linbit.linstor.core.objects.StorPoolControllerFactory;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionControllerFactory;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeConnectionFactory;
import com.linbit.linstor.core.objects.VolumeControllerFactory;
import com.linbit.linstor.core.objects.VolumeDefinitionControllerFactory;
import com.linbit.linstor.core.repository.FreeSpaceMgrRepository;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbcp.TestDbConnectionPoolLoader;
import com.linbit.linstor.dbcp.migration.AbsMigration;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.TestDbModule;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.layer.LayerSizeCalculatorModule;
import com.linbit.linstor.layer.resource.AbsRscLayerHelper;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.modularcrypto.CryptoProviderLoader;
import com.linbit.linstor.modularcrypto.JclCryptoModule;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.test.factories.NetInterfaceTestFactory;
import com.linbit.linstor.test.factories.NodeTestFactory;
import com.linbit.linstor.test.factories.ResourceDefinitionTestFactory;
import com.linbit.linstor.test.factories.ResourceGroupTestFactory;
import com.linbit.linstor.test.factories.ResourceTestFactory;
import com.linbit.linstor.test.factories.StorPoolDefinitionTestFactory;
import com.linbit.linstor.test.factories.StorPoolTestFactory;
import com.linbit.linstor.test.factories.VolumeDefinitionTestFactory;
import com.linbit.linstor.test.factories.VolumeTestFactory;
import com.linbit.linstor.transaction.ControllerTransactionMgrModule;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.ControllerSQLTransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.linstor.utils.NameShortenerModule;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.testing.fieldbinder.Bind;
import com.google.inject.testing.fieldbinder.BoundFieldModule;
import com.google.inject.util.Modules;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class GenericDbBase implements GenericDbTestConstants
{
    @Rule
    public TestName testMethodName = new TestName();

    @Rule
    // 10 seconds max per method tested
    public TestRule globalTimeout = new DisableOnDebug(Timeout.seconds(60));

    private static final String SELECT_PROPS_BY_INSTANCE =
        " SELECT " + PROPS_INSTANCE + ", " + PROP_KEY + ", " + PROP_VALUE +
        " FROM " + TBL_PROPS_CONTAINERS +
        " WHERE " + PROPS_INSTANCE + " = ? " +
        " ORDER BY " + PROP_KEY;

    private static final int PROPS_COL_ID_INSTANCE = 1;
    private static final int PROPS_COL_ID_KEY = 2;
    private static final int PROPS_COL_ID_VAL = 3;

    protected static StdErrorReporter errorReporter =
        new StdErrorReporter("TESTS", Paths.get("build/test-logs"), true, "", "INFO", "TRACE", () -> null);

    protected static final AccessContext SYS_CTX = DummySecurityInitializer.getSystemAccessContext();
    protected static final AccessContext PUBLIC_CTX = DummySecurityInitializer.getPublicAccessContext();

    protected static final AccessContext ALICE_ACC_CTX;
    protected static final AccessContext BOB_ACC_CTX;
    static
    {
        ALICE_ACC_CTX = TestAccessContextProvider.ALICE_ACC_CTX;
        BOB_ACC_CTX = TestAccessContextProvider.BOB_ACC_CTX;
    }

    // This connection pool is shared between the tests
    protected static DbConnectionPool dbConnPool;

    @Rule
    public final SeedDefaultPeerRule seedDefaultPeerRule = new SeedDefaultPeerRule();

    private List<Statement> statements = new ArrayList<>();
    private Connection con;
    private List<Connection> connections = new ArrayList<>();
    protected LinStorScope.ScopeAutoCloseable close;

    @Mock
    protected Peer mockPeer;

    @Mock
    @Bind
    @Named(NumberPoolModule.MINOR_NUMBER_POOL)
    protected DynamicNumberPool minorNrPoolMock;

    @Mock
    @Bind
    @Named(NumberPoolModule.TCP_PORT_POOL)
    protected DynamicNumberPool tcpPortPoolMock;

    @Mock
    @Bind
    @Named(NumberPoolModule.SPECIAL_SATELLTE_PORT_POOL)
    protected DynamicNumberPool openflexPortPoolMock;

    @Mock
    @Bind
    @Named(NumberPoolModule.SNAPSHOPT_SHIPPING_PORT_POOL)
    protected DynamicNumberPool snapshotShippingPortPoolMock;

    @Mock
    @Bind
    @Named(NumberPoolModule.LAYER_RSC_ID_POOL)
    protected DynamicNumberPool layerRscIdPoolMock;
    protected AtomicInteger layerRscIdAtomicId = new AtomicInteger();

    // mocked manually in setUp and bound in ApiTestModule
    protected SatelliteConnectorImpl satelliteConnector;

    // @Inject private DbAccessor secureDbDriver;
    // @Inject private DatabaseDriver persistenceDbDriver;
    @Inject
    private SecurityTestUtils securityTestUtils;
    @Inject
    protected CoreModule.NodesMap nodesMap;
    @Inject
    protected CoreModule.ResourceDefinitionMap rscDfnMap;
    @Inject
    protected CoreModule.StorPoolDefinitionMap storPoolDfnMap;
    @Inject
    protected CoreModule.ObjProtMap objProtMap;
    @Inject
    protected CoreModule.ExternalFileMap extFileMap;
    @Inject
    protected CoreModule.KeyValueStoreMap kvsMap;
    @Inject
    protected CoreModule.RemoteMap remoteMap;
    @Inject
    protected CoreModule.ResourceGroupMap rscGrpMap;
    @Inject
    protected CoreModule.ResourceDefinitionMapExtName rscDfnExtNameMap;
    @Inject
    protected CoreModule.ScheduleMap scheduleMap;

    @Inject
    protected NodeRepository nodeRepository;
    @Inject
    protected ResourceDefinitionRepository resourceDefinitionRepository;
    @Inject
    protected StorPoolDefinitionRepository storPoolDefinitionRepository;
    @Inject
    protected FreeSpaceMgrRepository freeSpaceMgrRepository;

    @Inject
    protected DatabaseLoader dbLoader;
    @Inject
    protected SecDatabaseLoader secDbLoader;

    @Inject
    protected ObjectProtectionFactory objectProtectionFactory;
    @Inject
    protected PropsContainerFactory propsContainerFactory;
    @Inject
    protected NodeControllerFactory nodeFactory;
    @Inject
    protected NodeTestFactory nodeTestFactory;
    @Inject
    protected ResourceConnectionControllerFactory resourceConnectionFactory;
    @Inject
    protected ResourceControllerFactory resourceFactory;
    @Inject
    protected ResourceTestFactory resourceTestFactory;
    @Inject
    protected StorPoolDefinitionControllerFactory storPoolDefinitionFactory;
    @Inject
    protected StorPoolDefinitionTestFactory storPoolDefinitionTestFactory;
    @Inject
    protected VolumeConnectionFactory volumeConnectionFactory;
    @Inject
    protected NodeConnectionFactory nodeConnectionFactory;
    @Inject
    protected StorPoolControllerFactory storPoolFactory;
    @Inject
    protected StorPoolTestFactory storPoolTestFactory;
    @Inject
    protected FreeSpaceMgrControllerFactory freeSpaceMgrFactory;
    @Inject
    protected VolumeControllerFactory volumeFactory;
    @Inject
    protected VolumeTestFactory volumeTestFactory;
    @Inject
    protected VolumeDefinitionControllerFactory volumeDefinitionFactory;
    @Inject
    protected VolumeDefinitionTestFactory volumeDefinitionTestFactory;
    @Inject
    protected ResourceDefinitionControllerFactory resourceDefinitionFactory;
    @Inject
    protected ResourceDefinitionTestFactory resourceDefinitionTestFactory;
    @Inject
    protected ResourceGroupControllerFactory resourceGroupFactory;
    @Inject
    protected ResourceGroupTestFactory resourceGroupTestFactory;
    @Inject
    protected SnapshotDefinitionControllerFactory snapshotDefinitionFactory;
    @Inject
    protected SnapshotVolumeDefinitionControllerFactory snapshotVolumeDefinitionFactory;
    @Inject
    protected SnapshotControllerFactory snapshotFactory;
    @Inject
    protected SnapshotVolumeControllerFactory snapshotVolumeFactory;
    @Inject
    protected NetInterfaceFactory netInterfaceFactory;
    @Inject
    protected NetInterfaceTestFactory netInterfaceTestFactory;

    @Inject
    protected LinStorScope testScope;
    @Inject
    protected TransactionObjectFactory transObjFactory;
    @Inject
    protected Provider<TransactionMgrSQL> transMgrProvider;

    @Inject
    protected ResourceGroupDatabaseDriver rscGrpDbDriver;

    @BeforeClass
    public static void setUpBeforeClass()
        throws DatabaseException, SQLException, InvalidNameException, InitializationException, AccessDeniedException,
        ClassNotFoundException
    {
        if (dbConnPool == null)
        {
            errorReporter.logTrace("Performing DB initialization");

            AbsMigration.setModularCryptoProvider(CryptoProviderLoader.loadCryptoProvider());

            TestDbConnectionPoolLoader dbConnectionPoolLoader = new TestDbConnectionPoolLoader();
            dbConnPool = dbConnectionPoolLoader.loadDbConnectionPool();

            dbConnPool.migrate(dbConnectionPoolLoader.getDbType());

            Identity.ensureDefaultsExist();
            SecurityType.ensureDefaultsExist();
            Role.ensureDefaultsExist();
        }

        // every test class must explicitly enable security if they want to test.
        // we should change this default to MAC once the security mechanism works as intended.
        SecurityLevel.set(SYS_CTX, SecurityLevel.NO_SECURITY, dbConnPool, null);
    }

    protected void setUpAndEnterScope() throws Exception
    {
        setUpWithoutEnteringScope(Modules.EMPTY_MODULE);
        enterScope();
        initAfterScopeWasEntered();
    }

    protected void initAfterScopeWasEntered() throws Exception
    {
        resourceGroupTestFactory.initDfltRscGrp();
        storPoolDefinitionTestFactory.initDfltDisklessStorPool();
    }

    protected void setUpWithoutEnteringScope(Module additionalModule) throws Exception
    {
        con = getNewConnection();

        errorReporter.logTrace("Running cleanups for next method: %s", testMethodName.getMethodName());
        truncateTables();
        insertDefaults(con);

        AbsRscLayerHelper.databaseLoadingFinished();
        errorReporter.logTrace("cleanups done, initializing: %s", testMethodName.getMethodName());

        MockitoAnnotations.initMocks(this);

        Mockito.when(mockPeer.getAccessContext()).thenReturn(PUBLIC_CTX);

        SatelliteConnectorImpl stltConnector = Mockito.mock(SatelliteConnectorImpl.class);
        Injector injector = Guice.createInjector(
            new GuiceConfigModule(),
            new LoggingModule(errorReporter),
            new TestSecurityModule(SYS_CTX),
            new ControllerLinstorModule(),
            new CoreModule(),
            new ControllerCoreModule(),
            new SharedDbConnectionPoolModule(),
            new TestDbModule(),
            new ControllerTransactionMgrModule(DatabaseDriverInfo.DatabaseType.SQL),
            new TestApiModule(),
            new ControllerSecurityModule(),
            new ApiCallHandlerModule(),
            new CtrlConfigModule(new CtrlConfig(null)),
            additionalModule,
            BoundFieldModule.of(this),
            new NameShortenerModule(),
            new JclCryptoModule(),
            new ApiTestModule(stltConnector),
            new LayerSizeCalculatorModule()
        );
        // super.setUpWithoutEnteringScope has also initialized satelliteConnector, but with a different
        // mock as we just gave guice to bind to. i.e. override with our local mock
        satelliteConnector = stltConnector;

        // injector.getInstance(DbCoreObjProtInitializer.class).initialize();
        injector.getInstance(DbDataInitializer.class).initialize();

        Mockito.when(layerRscIdPoolMock.autoAllocate()).then(ignoredContext -> layerRscIdAtomicId.getAndIncrement());

        injector.injectMembers(this);
    }

    protected void enterScope() throws Exception
    {
        TransactionMgrSQL transMgr = new ControllerSQLTransactionMgr(dbConnPool);
        // do not use scopes like this unless you absolutely need it for a test, use try-with-resource whenever possible
        close = testScope.enter();
        testScope.seed(TransactionMgr.class, transMgr);
        testScope.seed(TransactionMgrSQL.class, transMgr);
        if (seedDefaultPeerRule.shouldSeedDefaultPeer())
        {
            testScope.seed(
                Key.get(AccessContext.class, PeerContext.class),
                seedDefaultPeerRule.getDefaultPeerAccessContext()
            );
            testScope.seed(
                Key.get(AccessContext.class, ErrorReporterContext.class),
                seedDefaultPeerRule.getDefaultPeerAccessContext()
            );
            testScope.seed(Peer.class, mockPeer);
        }
    }

    @After
    public void tearDown() throws Exception
    {
        commitAndCleanUp(true);
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
        errorReporter.shutdown();
    }

    public void commitAndCleanUp(boolean inScope) throws Exception
    {
        if (inScope && transMgrProvider != null && transMgrProvider.get() != null)
        {
            transMgrProvider.get().commit();
        }
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
            if (inScope && testScope != null)
            {
                close.close();
            }
        }
    }

    protected void reloadSecurityObjects() throws AccessDeniedException, InitializationException, DatabaseException
    {
        // nodesMap.clear();
        // rscDfnMap.clear();
        // storPoolDfnMap.clear();
        // objProtMap.clear();
        // extFileMap.clear();
        // kvsMap.clear();
        // remoteMap.clear();
        // rscGrpMap.clear();
        // rscDfnExtNameMap.clear();
        // scheduleMap.clear();

        secDbLoader.loadAll();
        // dbLoader.loadSecurityObjects();
        // dbLoader.loadCoreObjects();
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
            sb.append(allContent.getString(PROPS_COL_ID_INSTANCE)).append(": ")
                .append(allContent.getString(PROPS_COL_ID_KEY)).append(" = ")
                .append(allContent.getString(PROPS_COL_ID_VAL)).append("\n");
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
    public void insertIdentity(TransactionMgrSQL transMgr, IdentityName name) throws SQLException
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
    public void insertSecType(TransactionMgrSQL transMgr, SecTypeName name) throws SQLException
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
    public void insertRole(TransactionMgrSQL transMgr, RoleName name, SecTypeName domain) throws SQLException
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
        TransactionMgrSQL transMgr,
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
        Node.Type... types
    )
        throws SQLException
    {
        long typeMask = 0;
        for (Node.Type type : types)
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
        TransactionMgrSQL transMgr,
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
        TransactionMgrSQL transMgr,
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
        TransactionMgrSQL transMgr,
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
        TransactionMgrSQL transMgr,
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
        TransactionMgrSQL transMgr,
        java.util.UUID uuid,
        ResourceName resName,
        ResourceDefinition.Flags... flags
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
        TransactionMgrSQL transMgr,
        java.util.UUID uuid,
        NodeName nodeName,
        ResourceName resName,
        NodeId nodeId,
        Resource.Flags... resFlags
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
        TransactionMgrSQL transMgr,
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
        TransactionMgrSQL transMgr,
        java.util.UUID uuid,
        NodeName nodeName,
        ResourceName resName,
        VolumeNumber volNr,
        StorPoolName storPoolName,
        String blockDev,
        String metaDisk,
        Volume.Flags... flags
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
        TransactionMgrSQL transMgr,
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
        TransactionMgrSQL transMgr,
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

    protected ResourceGroup createDefaultResourceGroup(AccessContext initCtx)
        throws InvalidNameException, AccessDeniedException, DatabaseException
    {
        ResourceGroup rscGrp = ((ResourceGroupDbDriver) rscGrpDbDriver).loadAll(null).keySet().stream()
            .filter(grp -> grp.getName().displayValue.equals(InternalApiConsts.DEFAULT_RSC_GRP_NAME))
            .findFirst()
            .get();
        rscGrp.getObjProt().addAclEntry(SYS_CTX, initCtx.subjectRole, AccessType.CONTROL);
        return rscGrp;
    }

    protected FreeSpaceMgr getFreeSpaceMgr(StorPoolDefinition storPoolDfn, Node node)
        throws AccessDeniedException, DatabaseException, InvalidNameException
    {
        return freeSpaceMgrFactory.getInstance(
            SYS_CTX, new SharedStorPoolName(node.getName(), storPoolDfn.getName())
        );
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
