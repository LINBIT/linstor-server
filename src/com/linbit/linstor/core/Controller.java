package com.linbit.linstor.core;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.event.Level;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.SystemServiceStopException;
import com.linbit.TransactionMgr;
import com.linbit.WorkerPool;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MetaDataApi;
import com.linbit.linstor.ControllerPeerCtx;
import com.linbit.linstor.CoreServices;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbdrivers.DerbyDriver;
import com.linbit.linstor.debug.DebugConsole;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.netcom.TcpConnectorService;
import com.linbit.linstor.netcom.ssl.SslTcpConnectorService;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.proto.CommonMessageProcessor;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.Authentication;
import com.linbit.linstor.security.Authorization;
import com.linbit.linstor.security.DbDerbyPersistence;
import com.linbit.linstor.security.IdentityName;
import com.linbit.linstor.security.Initializer;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SecurityLevel;
import com.linbit.linstor.security.SignInException;
import com.linbit.linstor.tasks.GarbageCollectorTask;
import com.linbit.linstor.tasks.PingTask;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.linstor.tasks.TaskScheduleService;
import com.linbit.linstor.timer.CoreTimer;
import com.linbit.utils.Base64;
import com.linbit.utils.MathUtils;

/**
 * linstor controller prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Controller extends LinStor implements Runnable, CoreServices
{
    // System module information
    public static final String MODULE = "Controller";

    // Database configuration file path
    public static final String DB_CONF_FILE = "database.cfg";

    // Database connection URL configuration key
    public static final String DB_CONN_URL = "connection-url";

    // Random data size for automatic DRBD shared secret generation
    // The random data will be Base64 encoded, so the length of the
    // shared secret string will be (SECRET_LEN + 2) / 3 * 4
    private static final int DRBD_SHARED_SECRET_SIZE = 15;

    // Maximum time to wait for services to shut down
    private static final long SVC_SHUTDOWN_WAIT_TIME = 10000L;

    private static final String DB_CONTROLLER_PROPSCON_INSTANCE_NAME = "CTRLCFG";

    private static final String PROPSCON_KEY_NETCOM = "netcom";
    private static final String PROPSCON_KEY_NETCOM_BINDADDR = "bindaddress";
    private static final String PROPSCON_KEY_NETCOM_PORT = "port";
    private static final String PROPSCON_KEY_NETCOM_TYPE = "type";
    private static final String PROPSCON_KEY_NETCOM_TRUSTSTORE = "trustStore";
    private static final String PROPSCON_KEY_NETCOM_TRUSTSTORE_PASSWD = "trustStorePasswd";
    private static final String PROPSCON_KEY_NETCOM_KEYSTORE = "keyStore";
    private static final String PROPSCON_KEY_NETCOM_KEYSTORE_PASSWD = "keyStorePasswd";
    private static final String PROPSCON_KEY_NETCOM_KEY_PASSWD = "keyPasswd";
    private static final String PROPSCON_KEY_NETCOM_SSL_PROTOCOL = "sslProtocol";
    private static final String PROPSCON_NETCOM_TYPE_PLAIN = "plain";
    private static final String PROPSCON_NETCOM_TYPE_SSL = "ssl";
    static final String PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC = "defaultPlainConSvc";
    static final String PROPSCON_KEY_DEFAULT_SSL_CON_SVC = "defaultSslConSvc";

    private static final short DEFAULT_PEER_COUNT = 31;
    private static final long DEFAULT_AL_SIZE = 32;
    private static final int DEFAULT_AL_STRIPES = 1;
    private static final String DEFAULT_STOR_POOL_NAME = "DfltStorPool";

    // System security context
    private AccessContext sysCtx;

    // Public security context
    private AccessContext publicCtx;

    // Command line arguments
    private String[] args;

    // TODO
    final MetaDataApi metaData;

    final CtrlApiCallHandler apiCallHandler;

    // ============================================================
    // Worker thread pool & message processing dispatcher
    //
    private WorkerPool workerThrPool = null;
    private CommonMessageProcessor msgProc;

    // Authentication & Authorization subsystems
    private Authentication idAuthentication = null;
    private Authorization roleAuthorization = null;

    // ============================================================
    // Core system services
    //
    // Map of controllable system services
    private final Map<ServiceName, SystemService> systemServicesMap;

    // Database connection pool service
    final DbConnectionPool dbConnPool;

    // Satellite reconnector service
    private final TaskScheduleService taskScheduleService;

    // Map of connected peers
    private final Map<String, Peer> peerMap;

    // Map of network communications connectors
    final Map<ServiceName, TcpConnector> netComConnectors;

    // The current API type (e.g ProtoBuf)
    private final ApiType apiType;

    // Shutdown controls
    private boolean shutdownFinished;
    private ObjectProtection shutdownProt;

    // Synchronization lock for major global changes
    public final ReadWriteLock reconfigurationLock;

    // Synchronization lock for the configuration
    public final ReadWriteLock ctrlConfLock;

    // Synchronization locks for linstor object maps
    public final ReadWriteLock nodesMapLock;
    public final ReadWriteLock rscDfnMapLock;
    public final ReadWriteLock storPoolDfnMapLock;

    // Controller configuration properties
    Props ctrlConf;
    ObjectProtection ctrlConfProt;

    // ============================================================
    // LinStor objects
    //
    // Map of all managed nodes
    Map<NodeName, Node> nodesMap;
    ObjectProtection nodesMapProt;

    // Map of all resource definitions
    Map<ResourceName, ResourceDefinition> rscDfnMap;
    ObjectProtection rscDfnMapProt;

    // Map of all storage pools
    Map<StorPoolName, StorPoolDefinition> storPoolDfnMap;
    ObjectProtection storPoolDfnMapProt;

    private short defaultPeerCount = DEFAULT_PEER_COUNT;
    private long defaultAlSize = DEFAULT_AL_SIZE;
    private int defaultAlStripes = DEFAULT_AL_STRIPES;
    private String defaultStorPoolName = DEFAULT_STOR_POOL_NAME;

    private ReconnectorTask reconnectorTask;
    private PingTask pingTask;

    public Controller(AccessContext sysCtxRef, AccessContext publicCtxRef, String[] argsRef)
    {
        // Initialize synchronization
        reconfigurationLock = new ReentrantReadWriteLock(true);
        ctrlConfLock        = new ReentrantReadWriteLock(true);
        nodesMapLock        = new ReentrantReadWriteLock(true);
        rscDfnMapLock       = new ReentrantReadWriteLock(true);
        storPoolDfnMapLock  = new ReentrantReadWriteLock(true);

        // Initialize security contexts
        sysCtx = sysCtxRef;
        publicCtx = publicCtxRef;

        // Initialize command line arguments
        args = argsRef;

        metaData = new MetaData();

        // Initialize and collect system services
        systemServicesMap = new TreeMap<>();
        {
            CoreTimer timer = super.getTimer();
            systemServicesMap.put(timer.getInstanceName(), timer);
        }
        dbConnPool = new DbConnectionPool();
        systemServicesMap.put(dbConnPool.getInstanceName(), dbConnPool);

        // Initialize network communications connectors map
        netComConnectors = new TreeMap<>();

        // Initialize connected peers map
        peerMap = new TreeMap<>();

        apiType = ApiType.PROTOBUF;

        taskScheduleService = new TaskScheduleService(this);
        systemServicesMap.put(taskScheduleService.getInstanceName(), taskScheduleService);

        // Initialize LinStor objects maps
        nodesMap = new TreeMap<>();
        rscDfnMap = new TreeMap<>();
        storPoolDfnMap = new TreeMap<>();
        // the corresponding protectionObjects will be initialized in the initialize method
        // after the initialization of the database

        {
            AccessContext apiCtx = sysCtx.clone();
            try
            {
                apiCtx.getEffectivePrivs().enablePrivileges(
                    Privilege.PRIV_OBJ_VIEW,
                    Privilege.PRIV_OBJ_USE,
                    Privilege.PRIV_OBJ_CHANGE,
                    Privilege.PRIV_MAC_OVRD
                );
                apiCallHandler = new CtrlApiCallHandler(this, apiType, apiCtx);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                throw new ImplementationError(
                    "Could not create API handler's access context",
                    accDeniedExc
                );
            }
        }

        // Initialize shutdown controls
        shutdownFinished = false;
    }

    public void initialize(ErrorReporter errorLogRef)
        throws InitializationException, SQLException
    {
        try
        {
            reconfigurationLock.writeLock().lock();

            shutdownFinished = false;
            try
            {
                AccessContext initCtx = sysCtx.clone();
                initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

                // Initialize the error & exception reporting facility
                setErrorLog(initCtx, errorLogRef);


                // Initialize the worker thread pool
                try
                {
                    initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

                    int cpuCount = getCpuCount();
                    int thrCount = cpuCount <= MAX_CPU_COUNT ? cpuCount : MAX_CPU_COUNT;
                    int qSize = thrCount * getWorkerQueueFactor();
                    qSize = qSize > MIN_WORKER_QUEUE_SIZE ? qSize : MIN_WORKER_QUEUE_SIZE;
                    setWorkerThreadCount(initCtx, thrCount);
                    setWorkerQueueSize(initCtx, qSize);
                    workerThrPool = WorkerPool.initialize(
                        thrCount, qSize, true, "MainWorkerPool", getErrorReporter(),
                        dbConnPool
                    );
                    // Initialize the message processor
                    // errorLogRef.logInfo("Initializing API call dispatcher");
                    msgProc = new CommonMessageProcessor(this, workerThrPool);
                }
                catch (AccessDeniedException accessDeniedException)
                {
                    throw new ImplementationError(
                        "Creation of the initialization access context failed",
                        accessDeniedException
                    );
                }

                // Initialize the database connections
                errorLogRef.logInfo("Initializing the database connection pool");
                Properties dbProps = new Properties();
                try (InputStream dbPropsIn = new FileInputStream(DB_CONF_FILE))
                {
                    // Load database configuration
                    dbProps.loadFromXML(dbPropsIn);
                }
                catch (IOException ioExc)
                {
                    errorLogRef.reportError(ioExc);
                }
                try
                {
                    // TODO: determine which DBDriver to use
                    AccessContext privCtx = sysCtx.clone();
                    privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                    securityDbDriver = new DbDerbyPersistence(privCtx, errorLogRef);
                    persistenceDbDriver = new DerbyDriver(
                        privCtx,
                        errorLogRef,
                        nodesMap,
                        rscDfnMap,
                        storPoolDfnMap
                    );

                    String connectionUrl = dbProps.getProperty(
                        DB_CONN_URL,
                        persistenceDbDriver.getDefaultConnectionUrl()
                    );

                    // Connect the database connection pool to the database
                    dbConnPool.initializeDataSource(
                        connectionUrl,
                        dbProps
                    );
                }
                catch (SQLException sqlExc)
                {
                    errorLogRef.reportError(sqlExc);
                }

                errorLogRef.logInfo("Initializing authentication subsystem");
                try
                {
                    idAuthentication = new Authentication(initCtx, dbConnPool, securityDbDriver, errorLogRef);
                }
                catch (AccessDeniedException accExc)
                {
                    throw new ImplementationError(
                        "The initialization security context does not have the necessary " +
                        "privileges to create the authentication subsystem",
                        accExc
                    );
                }
                catch (NoSuchAlgorithmException algoExc)
                {
                    throw new InitializationException(
                        "Initialization of the authentication subsystem failed because the " +
                        "required hashing algorithm is not supported on this platform",
                        algoExc
                    );
                }

                errorLogRef.logInfo("Initializing authorization subsystem");
                try
                {
                    roleAuthorization = new Authorization(initCtx, dbConnPool, securityDbDriver);
                }
                catch (AccessDeniedException accExc)
                {
                    throw new ImplementationError(
                        "The initialization security context does not have the necessary " +
                        "privileges to create the authorization subsystem",
                        accExc
                    );
                }

                // Load security identities, roles, domains/types, etc.
                errorLogRef.logInfo("Loading security objects");
                try
                {
                    Initializer.load(initCtx, dbConnPool, securityDbDriver);
                }
                catch (SQLException | InvalidNameException | AccessDeniedException exc)
                {
                    errorLogRef.reportError(exc);
                }

                errorLogRef.logInfo(
                    "Current security level is %s",
                    SecurityLevel.get().name()
                );

                TransactionMgr transMgr = null;
                try
                {
                    transMgr = new TransactionMgr(dbConnPool);

                    // initializing ObjectProtections for nodeMap, rscDfnMap and storPoolMap
                    nodesMapProt = ObjectProtection.getInstance(
                        initCtx,
                        ObjectProtection.buildPath(this, "nodesMap"),
                        true,
                        transMgr
                    );
                    rscDfnMapProt = ObjectProtection.getInstance(
                        initCtx,
                        ObjectProtection.buildPath(this, "rscDfnMap"),
                        true,
                        transMgr
                    );
                    storPoolDfnMapProt = ObjectProtection.getInstance(
                        initCtx,
                        ObjectProtection.buildPath(this, "storPoolMap"),
                        true,
                        transMgr
                    );

                    // initializing controller serial propsCon + OP
                    ctrlConf = loadPropsCon(errorLogRef);
                    ctrlConfProt = ObjectProtection.getInstance(
                        initCtx,
                        ObjectProtection.buildPath(this, "conf"),
                        true,
                        transMgr
                    );

                    shutdownProt = ObjectProtection.getInstance(
                        initCtx,
                        ObjectProtection.buildPath(this, "shutdown"),
                        true,
                        transMgr
                    );

                    shutdownProt.setConnection(transMgr);
                    // Set CONTROL access for the SYSTEM role on shutdown
                    shutdownProt.addAclEntry(initCtx, initCtx.getRole(), AccessType.CONTROL);

                    transMgr.commit(true);
                }
                catch (Exception rollbackExc)
                {
                    if (transMgr != null)
                    {
                        transMgr.rollback();
                    }
                    errorLogRef.reportError(rollbackExc, sysCtx, null, "Controller initialization");
                }
                finally
                {
                    if (transMgr != null)
                    {
                        dbConnPool.returnConnection(transMgr.dbCon);
                    }
                }

                errorLogRef.logInfo("Initializing test APIs");
                LinStor.loadApiCalls(msgProc, this, this, apiType);

                // Initialize tasks
                {
                    reconnectorTask = new ReconnectorTask(this);
                    pingTask = new PingTask(this, reconnectorTask);
                    taskScheduleService.addTask(pingTask);
                    taskScheduleService.addTask(reconnectorTask);

                    taskScheduleService.addTask(new GarbageCollectorTask());
                }

                // Initialize the network communications service
                Props config = loadPropsCon(errorLogRef);
                if (config == null)
                {
                    // we could not load the props - the loadPropsCon(..) should have
                    // reported the error already
                    // as we didn't start any services yet,
                    // TODO: we should retry in a minute or so
                }
                else
                {
                    errorLogRef.logInfo("Initializing network communications services");
                    try
                    {
                        initNetComServices(
                            config.getNamespace(PROPSCON_KEY_NETCOM),
                            errorLogRef
                        );
                    }
                    catch (InvalidKeyException e)
                    {
                        errorLogRef.reportError(
                            new ImplementationError(
                                "Constant key is invalid: " + PROPSCON_KEY_NETCOM,
                                e
                            )
                        );
                    }
                }
                // Initialize system services
                startSystemServices(systemServicesMap.values());

//                for(Entry<ServiceName, TcpConnector> entry : netComConnectors.entrySet())
//                {
//                    if (entry.getKey().value.contains("SSL"))
//                    {
//                        connectSatellite(new InetSocketAddress("localhost", 9978), entry.getValue());
//                    }
//                }
//                for(Entry<ServiceName, TcpConnector> entry : netComConnectors.entrySet())
//                {
//                    if (!entry.getKey().value.contains("SSL"))
//                    {
//                        connectSatellite(new InetSocketAddress("localhost", 9977), entry.getValue());
//                    }
//                }

                errorLogRef.logInfo("Core objects load from database is in progress");
                loadCoreObjects(initCtx);
                errorLogRef.logInfo("Core objects load from database completed");
            }
            catch (AccessDeniedException accessExc)
            {
                errorLogRef.reportError(
                    new ImplementationError(
                        "The initialization security context does not have all privileges. " +
                        "Initialization failed.",
                        accessExc
                    )
                );
            }
        }
        finally
        {
            reconfigurationLock.writeLock().unlock();
        }
    }

    public void loadCoreObjects(AccessContext initCtx)
        throws AccessDeniedException
    {
        TransactionMgr transMgr = null;
        Lock recfgWriteLock = reconfigurationLock.writeLock();
        try
        {
            transMgr = new TransactionMgr(dbConnPool);
            if (dbConnPool != null)
            {
                nodesMapProt.requireAccess(initCtx, AccessType.CONTROL);
                rscDfnMapProt.requireAccess(initCtx, AccessType.CONTROL);
                storPoolDfnMapProt.requireAccess(initCtx, AccessType.CONTROL);

                try
                {

                    // Replacing the entire configuration requires locking out all other tasks
                    //
                    // Since others task that use the configuration must hold the reconfiguration lock
                    // in read mode before locking any of the other system objects, locking the maps
                    // for nodes, resource definition, storage pool definitions, etc. can be skipped.
                    recfgWriteLock.lock();

                    // Clear the maps of any existing objects
                    //
                    // TODO: It would be better to keep the current configuration while trying to
                    //       load a new configuration, and only if loading the new configuration succeeded,
                    //       clear the old configuration and replace it with the new one
                    nodesMap.clear();
                    rscDfnMap.clear();
                    storPoolDfnMap.clear();

                    // Reload all objects
                    //
                    // FIXME: Loading or reloading the configuration must ensure to either load everything
                    //        or nothing to prevent ending up with a half-loaded configuration.
                    //        See also the TODO above.
                    persistenceDbDriver.loadAll(transMgr);
                }
                finally
                {
                    recfgWriteLock.unlock();
                }
            }
        }
        catch (SQLException exc)
        {
            getErrorReporter().reportError(
                Level.ERROR,
                new InitializationException(
                    "Loading the core objects from the database failed",
                    exc
                )
            );
        }
        finally
        {
            if (transMgr != null)
            {
                try
                {
                    transMgr.rollback();
                }
                catch (Exception ignored)
                {
                }
                if (dbConnPool != null)
                {
                    dbConnPool.returnConnection(transMgr.dbCon);
                }
            }
        }
    }

    @Override
    public void setSecurityLevel(AccessContext accCtx, SecurityLevel newLevel)
        throws AccessDeniedException, SQLException
    {
        SecurityLevel.set(
            accCtx, newLevel, dbConnPool, securityDbDriver
        );
    }

    @Override
    public void run()
    {
        ErrorReporter errLog = getErrorReporter();
        try
        {
            errLog.logInfo("Entering debug console");

            AccessContext privCtx = sysCtx.clone();
            AccessContext debugCtx = sysCtx.clone();
            privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
            debugCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            DebugConsole dbgConsole = createDebugConsole(privCtx, debugCtx, null);
            dbgConsole.stdStreamsConsole(CtrlDebugConsoleImpl.CONSOLE_PROMPT);
            System.out.println();

            errLog.logInfo("Debug console exited");
        }
        catch (Throwable error)
        {
            getErrorReporter().reportError(error);
        }

        try
        {
            AccessContext shutdownCtx = sysCtx.clone();
            // Just in case that someone removed the access control list entry
            // for the system's role or changed the security type for shutdown,
            // override access controls with the system context's privileges
            shutdownCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_OBJ_USE, Privilege.PRIV_MAC_OVRD);
            shutdown(shutdownCtx);
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "Cannot shutdown() using the system's security context. " +
                "Suspected removal of privileges from the system context.",
                accExc
            );
        }
    }

    public void shutdown(AccessContext accCtx) throws AccessDeniedException
    {
        shutdown(accCtx, true);
    }

    public void shutdown(AccessContext accCtx, boolean sysExit) throws AccessDeniedException
    {
        shutdownProt.requireAccess(accCtx, AccessType.USE);

        ErrorReporter errLog = getErrorReporter();

        try
        {
            reconfigurationLock.writeLock().lock();
            if (!shutdownFinished)
            {
                errLog.logInfo(
                    String.format(
                        "Shutdown initiated by subject '%s' using role '%s'\n",
                        accCtx.getIdentity(), accCtx.getRole()
                    )
                );

                errLog.logInfo("Shutdown in progress");

                // Shutdown service threads
                stopSystemServices(systemServicesMap.values());

                if (workerThrPool != null)
                {
                    errLog.logInfo("Shutting down worker thread pool");
                    workerThrPool.shutdown();
                    workerThrPool = null;
                }

                long exitTime = MathUtils.addExact(System.currentTimeMillis(), SVC_SHUTDOWN_WAIT_TIME);
                for (SystemService svc  : systemServicesMap.values())
                {
                    long now = System.currentTimeMillis();
                    if (now < exitTime)
                    {
                        long maxWaitTime = exitTime - now;
                        if (maxWaitTime > SVC_SHUTDOWN_WAIT_TIME)
                        {
                            maxWaitTime = SVC_SHUTDOWN_WAIT_TIME;
                        }

                        try
                        {
                            svc.awaitShutdown(maxWaitTime);
                        }
                        catch (InterruptedException ignored)
                        {
                        }
                        catch (Throwable error)
                        {
                            errLog.reportError(Level.ERROR, error);
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                errLog.logInfo("Shutdown complete");
            }
            shutdownFinished = true;
        }
        catch (Throwable error)
        {
            errLog.reportError(Level.ERROR, error);
        }
        finally
        {
            reconfigurationLock.writeLock().unlock();
        }
        if (sysExit)
        {
            System.exit(0);
        }
    }

    public void peerSignIn(
        Peer client,
        IdentityName idName,
        byte[] password
    )
        throws SignInException, InvalidNameException
    {
        AccessContext peerSignInCtx = idAuthentication.signIn(idName, password);
        try
        {
            AccessContext privCtx = sysCtx.clone();
            privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
            client.setAccessContext(privCtx, peerSignInCtx);
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "Enabling privileges on the system context failed",
                accExc
            );
        }
    }

    /**
     * Creates a debug console instance for remote use by a connected peer
     *
     * @param accCtx The access context to authorize this API call
     * @param client Connected peer
     * @return New DebugConsole instance
     * @throws AccessDeniedException If the API call is not authorized
     */
    public DebugConsole createDebugConsole(
        AccessContext accCtx,
        AccessContext debugCtx,
        Peer client
    )
        throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
        CtrlDebugConsoleImpl peerDbgConsole = new CtrlDebugConsoleImpl(
            this,
            debugCtx,
            systemServicesMap,
            peerMap,
            msgProc
        );
        if (client != null)
        {
            ControllerPeerCtx peerContext = (ControllerPeerCtx) client.getAttachment();
            // Initialize remote debug console
            // FIXME: loadDefaultCommands() should not use System.out and System.err
            //        if the debug console is created for a peer / client
            peerDbgConsole.loadDefaultCommands(System.out, System.err);
            peerContext.setDebugConsole(peerDbgConsole);
        }
        else
        {
            // Initialize local debug console
            peerDbgConsole.loadDefaultCommands(System.out, System.err);
        }

        return peerDbgConsole;
    }

    public static void printField(String fieldName, String fieldContent)
    {
        System.out.printf("  %-32s: %s\n", fieldName, fieldContent);
    }

    public static void main(String[] args)
    {
        System.out.printf(
            "%s, Module %s, Release %s\n",
            Controller.PROGRAM, Controller.MODULE, Controller.VERSION
        );
        printStartupInfo();

        ErrorReporter errorLog = new StdErrorReporter(Controller.MODULE);

        try
        {
            Thread.currentThread().setName("Main");

            // Initialize the Controller module with the SYSTEM security context
            Initializer sysInit = new Initializer();
            Controller instance = sysInit.initController(args);

            instance.initialize(errorLog);
            instance.run();
        }
        catch (InitializationException initExc)
        {
            errorLog.reportError(initExc);
            System.err.println("Initialization of the Controller module failed.");
        }
        catch (ImplementationError implError)
        {
            errorLog.reportError(implError);
        }
        catch (Throwable error)
        {
            errorLog.reportError(error);
        }

        System.out.println();
    }

    private Props loadPropsCon(ErrorReporter errorLogRef)
    {
        Props config = null;
        TransactionMgr transMgr = null;
        try
        {
            transMgr = new TransactionMgr(dbConnPool);
            config = PropsContainer.getInstance(DB_CONTROLLER_PROPSCON_INSTANCE_NAME, transMgr);
            transMgr.commit();
        }
        catch (SQLException sqlExc)
        {
            if (transMgr == null)
            {
                errorLogRef.reportError(
                    new SystemServiceStartException("Failed to create transaction when loading controller's properties")
                );
            }
            else
            {
                try
                {
                    transMgr.rollback();
                }
                catch (SQLException rollbackExc)
                {
                    errorLogRef.reportError(
                        rollbackExc,
                        null,
                        null,
                        "Transaction rollback threw an exception"
                    );
                }
                errorLogRef.reportError(
                    new SystemServiceStartException(
                        "Failed to load controller's config from the database",
                        "Failed to load PropsContainer from the database",
                        sqlExc.getLocalizedMessage(),
                        null,
                        null,
                        sqlExc
                    )
                );
            }
        }
        finally
        {
            dbConnPool.returnConnection(transMgr);
        }
        return config;
    }

    private List<TcpConnector> initNetComServices(Props netComProps, ErrorReporter errorLogRef)
    {
        List<TcpConnector> tcpCons = new ArrayList<>();
        if (netComProps == null)
        {
            String errorMsg = "The controller configuration does not define any network communication services";
            errorLogRef.reportError(
                new SystemServiceStartException(
                    errorMsg,
                    errorMsg,
                    null,
                    null,
                    "Define at least one network communication service",
                    null
                )
            );
        }
        else
        {
            Iterator<String> namespaces = netComProps.iterateNamespaces();
            while (namespaces.hasNext())
            {
                try
                {
                    String namespaceStr = namespaces.next();
                    TcpConnector netComSvc = createNetComService(namespaceStr, netComProps, errorLogRef);
                    if (netComSvc != null)
                    {
                        tcpCons.add(netComSvc);
                    }
                }
                catch (SystemServiceStartException sysSvcStartExc)
                {
                    errorLogRef.reportProblem(Level.ERROR, sysSvcStartExc, null, null, null);
                }
            }
        }
        return tcpCons;
    }

    TcpConnector createNetComService(
        String serviceNameStr,
        Props netComProps,
        ErrorReporter errorLogRef
    )
        throws SystemServiceStartException
    {
        ServiceName serviceName;
        try
        {
            serviceName = new ServiceName(serviceNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new SystemServiceStartException(
                String.format(
                    "The name '%s' can not be used for a network communication service instance",
                    serviceNameStr
                ),
                String.format(
                    "The name '%s' is not a valid name for a network communication service instance",
                    serviceNameStr
                ),
                null,
                "Change the name of the network communication service instance",
                null,
                invalidNameExc
            );
        }
        Props configProp;
        try
        {
            configProp = netComProps.getNamespace(serviceNameStr);
        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ImplementationError(
                String.format(
                    "A properties container returned the key '%s' as the identifier for a namespace, " +
                    "but using the same key to obtain a reference to the namespace generated an " +
                    "%s",
                    serviceName,
                    invalidKeyExc.getClass().getSimpleName()
                ),
                invalidKeyExc
            );
        }
        String bindAddressStr = loadPropChecked(configProp, PROPSCON_KEY_NETCOM_BINDADDR);
        Integer port = Integer.parseInt(loadPropChecked(configProp, PROPSCON_KEY_NETCOM_PORT));
        String type = loadPropChecked(configProp, PROPSCON_KEY_NETCOM_TYPE);

        SocketAddress bindAddress = new InetSocketAddress(bindAddressStr, port);

        TcpConnector netComSvc = null;
        if (type.equals(PROPSCON_NETCOM_TYPE_PLAIN))
        {
            netComSvc = new TcpConnectorService(
                this,
                msgProc,
                bindAddress,
                publicCtx,
                new CtrlConnTracker(
                    this,
                    peerMap,
                    reconnectorTask
                )
            );
            try
            {
                if (ctrlConf.getProp(PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC) == null)
                {
                    TransactionMgr transMgr = null;
                    try
                    {
                        transMgr = new TransactionMgr(dbConnPool);
                        ctrlConf.setConnection(transMgr);
                        ctrlConf.setProp(PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC, serviceName.displayValue);
                        transMgr.commit();
                    }
                    catch (SQLException sqlExc)
                    {
                        errorLogRef.reportError(
                            sqlExc,
                            sysCtx,
                            null,
                            "An SQL exception was thrown while trying to persist the default plain connector"
                        );
                    }
                    finally
                    {
                        if (transMgr != null)
                        {
                            try
                            {
                                transMgr.rollback();
                            }
                            catch (SQLException sqlExc2)
                            {
                                errorLogRef.reportError(
                                    sqlExc2,
                                    sysCtx,
                                    null,
                                    "An SQL exception was thrown while trying to rollback a transaction"
                                );
                            }
                            dbConnPool.returnConnection(transMgr);
                        }
                    }
                }
            }
            catch (AccessDeniedException | InvalidKeyException | InvalidValueException exc)
            {
                errorLogRef.reportError(
                    new ImplementationError(
                        "Storing default plain connector service caused exception",
                        exc
                    )
                );
            }
        }
        else
        if (type.equals(PROPSCON_NETCOM_TYPE_SSL))
        {
            try
            {
                netComSvc = new SslTcpConnectorService(
                    this,
                    msgProc,
                    bindAddress ,
                    publicCtx,
                    new CtrlConnTracker(
                        this,
                        peerMap,
                        reconnectorTask
                    ),
                    loadPropChecked(configProp, PROPSCON_KEY_NETCOM_SSL_PROTOCOL),
                    loadPropChecked(configProp, PROPSCON_KEY_NETCOM_KEYSTORE),
                    loadPropChecked(configProp, PROPSCON_KEY_NETCOM_KEYSTORE_PASSWD).toCharArray(),
                    loadPropChecked(configProp, PROPSCON_KEY_NETCOM_KEY_PASSWD).toCharArray(),
                    loadPropChecked(configProp, PROPSCON_KEY_NETCOM_TRUSTSTORE),
                    loadPropChecked(configProp, PROPSCON_KEY_NETCOM_TRUSTSTORE_PASSWD).toCharArray()
                );
                try
                {
                    if (ctrlConf.getProp(PROPSCON_KEY_DEFAULT_SSL_CON_SVC) == null)
                    {

                        TransactionMgr transMgr = null;
                        try
                        {
                            transMgr = new TransactionMgr(dbConnPool);
                            ctrlConf.setConnection(transMgr);
                            ctrlConf.setProp(PROPSCON_KEY_DEFAULT_SSL_CON_SVC, serviceName.displayValue);
                            transMgr.commit();
                        }
                        catch (SQLException sqlExc)
                        {
                            errorLogRef.reportError(
                                sqlExc,
                                sysCtx,
                                null,
                                "An SQL exception was thrown while trying to persist the default ssl connector"
                            );
                        }
                        finally
                        {
                            if (transMgr != null)
                            {
                                try
                                {
                                    transMgr.rollback();
                                }
                                catch (SQLException sqlExc2)
                                {
                                    errorLogRef.reportError(
                                        sqlExc2,
                                        sysCtx,
                                        null,
                                        "An SQL exception was thrown while trying to rollback a transaction"
                                    );
                                }
                                dbConnPool.returnConnection(transMgr);
                            }
                        }


                    }
                }
                catch (AccessDeniedException | InvalidKeyException | InvalidValueException exc)
                {
                    errorLogRef.reportError(
                        new ImplementationError(
                            "Storing default ssl connector service caused exception",
                            exc
                        )
                    );
                }
            }
            catch (
                KeyManagementException | UnrecoverableKeyException |
                NoSuchAlgorithmException | KeyStoreException | CertificateException |
                IOException exc
            )
            {
                String errorMsg = "Initialization of an SSL-enabled network communication service failed";
                errorLogRef.reportError(exc);
                throw new SystemServiceStartException(
                    errorMsg,
                    errorMsg,
                    null,
                    null,
                    null,
                    exc
                );
            }
        }
        else
        {
            errorLogRef.reportProblem(
                Level.ERROR,
                new LinStorException(
                    String.format(
                        "The connection type for the network communication service '%s' is not valid",
                        serviceName
                    ),
                    String.format(
                        "The connection type has to be either '%s' or '%s', but was '%s'",
                        PROPSCON_NETCOM_TYPE_PLAIN,
                        PROPSCON_NETCOM_TYPE_SSL,
                        type),
                    null,
                    "Correct the entry in the database",
                    null
                ),
                null, // accCtx
                null, // client
                null  // contextInfo
            );
        }

        if (netComSvc != null)
        {
            netComSvc.setServiceInstanceName(serviceName);
            netComConnectors.put(serviceName, netComSvc);
            systemServicesMap.put(serviceName, netComSvc);
            errorLogRef.logInfo(
                String.format(
                    "Created network communication service '%s', bound to %s:%d",
                    serviceName.displayValue, bindAddressStr, port
                )
            );
        }

        return netComSvc;
    }

    boolean deleteNetComService(String serviceNameStr, ErrorReporter errorLogRef) throws SystemServiceStopException
    {
        ServiceName serviceName;
        try
        {
            serviceName = new ServiceName(serviceNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new SystemServiceStopException(
                String.format(
                    "The name '%s' can not be used for a network communication service instance",
                    serviceNameStr
                ),
                String.format(
                    "The name '%s' is not a valid name for a network communication service instance",
                    serviceNameStr
                ),
                null,
                "Change the name of the network communication service instance",
                null,
                invalidNameExc
            );
        }
        TcpConnector netComSvc = netComConnectors.get(serviceName);
        SystemService sysSvc = systemServicesMap.get(serviceName);

        boolean svcStarted = false;
        boolean issuedShutdown = false;
        if (netComSvc != null)
        {
            svcStarted = netComSvc.isStarted();
            if (svcStarted)
            {
                netComSvc.shutdown();
                issuedShutdown = true;
            }
        }
        else
        if (sysSvc != null)
        {
            svcStarted = sysSvc.isStarted();
            if (svcStarted)
            {
                sysSvc.shutdown();
                issuedShutdown = true;
            }
        }

        netComConnectors.remove(serviceName);
        systemServicesMap.remove(serviceName);

        if (svcStarted && issuedShutdown)
        {
            errorLogRef.logInfo(
                String.format(
                    "Initiated shutdown of network communication service '%s'",
                    serviceName.displayValue
                )
            );
        }

        if (netComSvc != null || sysSvc != null)
        {
            errorLogRef.logInfo(
                String.format(
                    "Deleted network communication service '%s'",
                    serviceName.displayValue
                )
            );
        }

        return netComSvc != null || sysSvc != null;
    }

    private String loadPropChecked(Props props, String key) throws SystemServiceStartException
    {
        String value = null;
        try
        {
            value = props.getProp(key);
            if (value == null)
            {
                String errorMsg = String.format(
                    "The configuration entry '%s%s' is missing in the configuration",
                    props.getPath(), key
                );
                throw new SystemServiceStartException(
                    errorMsg,
                    errorMsg,
                    null,
                    "Add the missing configuration entry to the configuration",
                    null
                );
            }

        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ImplementationError("Constant key is invalid " + key, invalidKeyExc);
        }

        return value;
    }

    public void connectSatellite(
        final InetSocketAddress satelliteAddress,
        final TcpConnector tcpConnector,
        final Node node
    )
    {
        Runnable connectRunnable = new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    Peer peer = tcpConnector.connect(satelliteAddress, node);
                    {
                        AccessContext connectorCtx = sysCtx.clone();
                        connectorCtx.getEffectivePrivs().enablePrivileges(
                            Privilege.PRIV_MAC_OVRD,
                            Privilege.PRIV_OBJ_CHANGE
                        );
                        node.setPeer(connectorCtx, peer);
                    }
                    if (!peer.isConnected())
                    {
                        reconnectorTask.add(peer);
                    }
                    else
                    {
                        pingTask.add(peer);
                    }
                }
                catch (IOException ioExc)
                {
                    getErrorReporter().reportError(
                        new LinStorException(
                            "Cannot connect to satellite",
                            String.format(
                                "Establishing connection to satellite (%s:%d) failed",
                                satelliteAddress.getAddress().getHostAddress(),
                                satelliteAddress.getPort()
                            ),
                            "IOException occured. See cause for further details",
                            null,
                            null,
                            ioExc
                        )
                    );
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    getErrorReporter().reportError(
                        new ImplementationError(
                            "System context has not enough privileges to set peer for a connecting node",
                            accDeniedExc
                        )
                    );
                    accDeniedExc.printStackTrace();
                }
            }
        };

        workerThrPool.submit(connectRunnable);
    }

    /**
     * Generates a random value for a DRBD resource's shared secret
     *
     * @return a 20 character long random String
     */
    public String generateSharedSecret()
    {
        byte[] randomBytes = new byte[DRBD_SHARED_SECRET_SIZE];
        new SecureRandom().nextBytes(randomBytes);
        String secret = Base64.encode(randomBytes);
        return secret;
    }

    public MetaDataApi getMetaDataApi()
    {
        return metaData;
    }

    public short getDefaultPeerCount()
    {
        return defaultPeerCount;
    }

    public int getDefaultAlStripes()
    {
        return defaultAlStripes;
    }

    public long getDefaultAlSize()
    {
        return defaultAlSize;
    }

    public String getDefaultStorPoolName()
    {
        return defaultStorPoolName;
    }

    public CtrlApiCallHandler getApiCallHandler()
    {
        return apiCallHandler;
    }
}
