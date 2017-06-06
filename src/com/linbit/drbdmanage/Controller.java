package com.linbit.drbdmanage;

import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.WorkerPool;
import com.linbit.drbdmanage.debug.BaseDebugConsole;
import com.linbit.drbdmanage.debug.CommonDebugCmd;
import com.linbit.drbdmanage.debug.ControllerDebugCmd;
import com.linbit.drbdmanage.debug.DebugConsole;
import com.linbit.drbdmanage.dbcp.DbConnectionPool;
import com.linbit.drbdmanage.dbdrivers.DatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.DerbyDriver;
import com.linbit.drbdmanage.logging.StdErrorReporter;
import com.linbit.drbdmanage.netcom.ConnectionObserver;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnector;
import com.linbit.drbdmanage.netcom.TcpConnectorService;
import com.linbit.drbdmanage.netcom.ssl.SslTcpConnectorService;
import com.linbit.drbdmanage.propscon.InvalidKeyException;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsConDatabaseDriver;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;
import com.linbit.drbdmanage.proto.CommonMessageProcessor;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.Authentication;
import com.linbit.drbdmanage.security.DbAccessor;
import com.linbit.drbdmanage.security.DbDerbyPersistence;
import com.linbit.drbdmanage.security.Identity;
import com.linbit.drbdmanage.security.IdentityName;
import com.linbit.drbdmanage.security.Initializer;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.Privilege;
import com.linbit.drbdmanage.timer.CoreTimer;
import com.linbit.utils.Base64;

import java.io.FileInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.event.Level;

/**
 * drbdmanageNG controller prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Controller extends DrbdManage implements Runnable, CoreServices
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

    // System security context
    private AccessContext sysCtx;

    // Public security context
    private AccessContext publicCtx;

    // Command line arguments
    private String[] args;

    // ============================================================
    // Worker thread pool & message processing dispatcher
    //
    private WorkerPool workerThrPool = null;
    private CommonMessageProcessor msgProc = null;

    // Authentication subsystem
    private Authentication auth = null;

    // ============================================================
    // Core system services
    //
    // Map of controllable system services
    private final Map<ServiceName, SystemService> systemServicesMap;

    // Database connection pool service
    private final DbConnectionPool dbConnPool;

    // Map of connected peers
    private final Map<String, Peer> peerMap;

    // Map of network communications connectors
    private final Map<ServiceName, TcpConnector> netComConnectors;

    // Database drivers
    private DbAccessor securityDbDriver;
    private DatabaseDriver persistenceDbDriver;

    // Shutdown controls
    private boolean shutdownFinished;
    private ObjectProtection shutdownProt;

    // Lock for major global changes
    private final ReadWriteLock reconfigurationLock;

    // Controller configuration properties
    private Props ctrlConf;
    private ObjectProtection ctrlConfProt;

    public Controller(AccessContext sysCtxRef, AccessContext publicCtxRef, String[] argsRef)
    {
        // Initialize synchronization
        reconfigurationLock = new ReentrantReadWriteLock(true);

        // Initialize security contexts
        sysCtx = sysCtxRef;
        publicCtx = publicCtxRef;

        // Initialize command line arguments
        args = argsRef;

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

        // Initialize shutdown controls
        shutdownFinished = false;
        shutdownProt = new ObjectProtection(sysCtx);
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

                // Set CONTROL access for the SYSTEM role on shutdown
                shutdownProt.addAclEntry(initCtx, sysCtx.getRole(), AccessType.CONTROL);

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
                    securityDbDriver = new DbDerbyPersistence();
                    persistenceDbDriver = new DerbyDriver(errorLogRef);

                    String connectionUrl = dbProps.getProperty(
                        DB_CONN_URL,
                        persistenceDbDriver.getDefaultConnectionUrl()
                    );

                    // Connect the database connection pool to the database
                    dbConnPool.setServiceInstanceName(
                        persistenceDbDriver.getDefaultServiceInstanceName()
                    );
                    dbConnPool.initializeDataSource(
                        connectionUrl,
                        dbProps
                    );
                }
                catch (SQLException sqlExc)
                {
                    errorLogRef.reportError(sqlExc);
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

                errorLogRef.logInfo("Initializing authentication subsystem");
                try
                {
                    auth = new Authentication(initCtx, dbConnPool, securityDbDriver);
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

                ctrlConf = SerialPropsContainer.createRootContainer(
                    persistenceDbDriver.getPropsConDatabaseDriver(DB_CONTROLLER_PROPSCON_INSTANCE_NAME, dbConnPool));
                ctrlConfProt = new ObjectProtection(sysCtx);

                // Initialize the worker thread pool
                errorLogRef.logInfo("Starting worker thread pool");
                {
                    int cpuCount = getCpuCount();
                    int thrCount = cpuCount <= MAX_CPU_COUNT ? cpuCount : MAX_CPU_COUNT;
                    int qSize = thrCount * getWorkerQueueFactor();
                    qSize = qSize > MIN_WORKER_QUEUE_SIZE ? qSize : MIN_WORKER_QUEUE_SIZE;
                    setWorkerThreadCount(initCtx, thrCount);
                    setWorkerQueueSize(initCtx, qSize);
                    workerThrPool = WorkerPool.initialize(
                        thrCount, qSize, true, "MainWorkerPool", getErrorReporter()
                    );
                }

                // Initialize the message processor
                errorLogRef.logInfo("Initializing API call dispatcher");
                msgProc = new CommonMessageProcessor(this, workerThrPool);

                errorLogRef.logInfo("Initializing test APIs");
                DrbdManage.loadApiCalls(msgProc, this, this);
//                {
//                    msgProc.addApiCall(new Ping(this, this));
//                    msgProc.addApiCall(new CreateDebugConsole(this, this));
//                    msgProc.addApiCall(new DestroyDebugConsole(this, this));
//                    msgProc.addApiCall(new DebugCommand(this, this));
//                    msgProc.addApiCall(new DebugMakeSuperuser(this, this));
//                }

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
                        initNetComServices(config.getNamespace(PROPSCON_KEY_NETCOM), errorLogRef);
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
                // Initialize the network communications service




                // Initialize system services
                startSystemServices(systemServicesMap.values());
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

            DebugConsole dbgConsole = createDebugConsole(privCtx, debugCtx, null);
            dbgConsole.stdStreamsConsole(DebugConsoleImpl.CONSOLE_PROMPT);
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

                errLog.logInfo("Shutdown complete");
            }
            shutdownFinished = true;
        }
        finally
        {
            reconfigurationLock.writeLock().unlock();
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
        Controller.DebugConsoleImpl peerDbgConsole = new Controller.DebugConsoleImpl(this, debugCtx);
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

    /**
     * FIXME: DEBUG CODE, Overrides security -- Remove before production use
     *
     * @param client The peer client to set a privileged security context on
     * @return True if the operation succeeded, false otherwise
     */
    public boolean debugMakePeerPrivileged(Peer client)
    {
        boolean successFlag = false;
        try
        {
            AccessContext impCtx = sysCtx.clone();
            impCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            AccessContext privPeerCtx = impCtx.impersonate(
                Identity.create(impCtx, new IdentityName("DebugClient")),
                sysCtx.getRole(),
                sysCtx.getDomain(),
                Privilege.PRIV_SYS_ALL
            );
            client.setAccessContext(impCtx, privPeerCtx);
            successFlag = true;
        }
        catch (InvalidNameException nameExc)
        {
            getErrorReporter().reportError(nameExc);
        }
        catch (AccessDeniedException accessExc)
        {
            getErrorReporter().reportError(accessExc);
        }
        return successFlag;
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
        try
        {
            PropsConDatabaseDriver propsConDbDriver = persistenceDbDriver.getPropsConDatabaseDriver(
                DB_CONTROLLER_PROPSCON_INSTANCE_NAME, dbConnPool
            );
            config = PropsContainer.loadContainer(propsConDbDriver);
        }
        catch (SQLException sqlExc)
        {
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
        catch (InvalidKeyException invalidPropExc)
        {
            errorLogRef.reportError(
                new SystemServiceStartException(
                    "Failed to load controller's config from the database",
                    "Failed to load PropsContainer from the database",
                    invalidPropExc.getLocalizedMessage(),
                    null,
                    null,
                    invalidPropExc
               )
            );
        }
        return config;
    }

    private void initNetComServices(Props netComProps, ErrorReporter errorLog)
    {
        if (netComProps == null)
        {
            errorLog.reportError(
                new SystemServiceStartException(
                    "No netCom services defined",
                    "The propsContainer loaded from the database did not contain any netCom services",
                    null,
                    "Inset at least one netCom service to the database",
                    null
                )
            );
        }
        else
        {
            Iterator<String> namespaces = netComProps.iterateNamespaces();
            while (namespaces.hasNext())
            {
                String namespaceStr = namespaces.next();
                ServiceName serviceName;
                try
                {
                    serviceName = new ServiceName(namespaceStr);
                }
                catch (InvalidNameException invalidNameExc)
                {
                    errorLog.reportError(
                        new SystemServiceStartException(
                            "Invalid sevice name",
                            "The given name is not a valid service name: " + namespaceStr,
                            null,
                            "Correct the name (entries) in the database",
                            null,
                            invalidNameExc
                        )
                    );
                    continue; // netComSvc cannot be initialized without serviceName
                }
                Props configProp;
                try
                {
                    configProp = netComProps.getNamespace(namespaceStr);
                }
                catch (InvalidKeyException invalidKeyExc)
                {
                    errorLog.reportError(
                        new SystemServiceStartException(
                            "Invalid namespace name",
                            "The given name is not a valid name for a propsContainer namespace: " + namespaceStr,
                            null,
                            "Correct the name (entries) in the database",
                            null,
                            invalidKeyExc
                        )
                    );
                    continue;
                }
                String bindAddressStr;
                int port;
                String type;
                try
                {
                    bindAddressStr = loadPropChecked(configProp, PROPSCON_KEY_NETCOM_BINDADDR);
                    port = Integer.parseInt(loadPropChecked(configProp, PROPSCON_KEY_NETCOM_PORT));
                    type = loadPropChecked(configProp, PROPSCON_KEY_NETCOM_TYPE);
                }
                catch (SystemServiceStartException sysSvcStartExc)
                {
                    errorLog.reportError(sysSvcStartExc);
                    continue;
                }

                SocketAddress bindAddress = new InetSocketAddress(bindAddressStr, port);

                TcpConnector netComSvc = null;
                if (type.equals(PROPSCON_NETCOM_TYPE_PLAIN))
                {
                    netComSvc = new TcpConnectorService(
                        this,
                        msgProc,
                        bindAddress ,
                        publicCtx,
                        new ConnTracker(null)
                    );
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
                            new ConnTracker(null),
                            loadPropChecked(configProp, PROPSCON_KEY_NETCOM_SSL_PROTOCOL),
                            loadPropChecked(configProp, PROPSCON_KEY_NETCOM_KEYSTORE),
                            loadPropChecked(configProp, PROPSCON_KEY_NETCOM_KEYSTORE_PASSWD).toCharArray(),
                            loadPropChecked(configProp, PROPSCON_KEY_NETCOM_KEY_PASSWD).toCharArray(),
                            loadPropChecked(configProp, PROPSCON_KEY_NETCOM_TRUSTSTORE),
                            loadPropChecked(configProp, PROPSCON_KEY_NETCOM_TRUSTSTORE_PASSWD).toCharArray()
                        );
                    }
                    catch (
                        KeyManagementException | UnrecoverableKeyException |
                        NoSuchAlgorithmException | KeyStoreException | CertificateException |
                        IOException exc
                    )
                    {
                        errorLog.reportError(
                            new SystemServiceStartException(
                                "Cannot start ssl TCP connector service",
                                "Constructing a new ssl TCP connector service failed",
                                exc.getLocalizedMessage(),
                                null,
                                null,
                                exc
                            )
                        );
                        continue;
                    }
                    catch (SystemServiceStartException sysSvcStartExc)
                    {
                        errorLog.reportError(sysSvcStartExc);
                        continue;
                    }
                }
                if (netComSvc == null)
                {
                    errorLog.reportProblem(
                        Level.ERROR,
                        new DrbdManageException(
                            "Invalid connection type",
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
                else
                {
                    netComSvc.setServiceInstanceName(serviceName);
                    netComConnectors.put(serviceName, netComSvc);
                    systemServicesMap.put(serviceName, netComSvc);
                    try
                    {
                        netComSvc.start();
                    }
                    catch (SystemServiceStartException sysSvcStartExc)
                    {
                        errorLog.reportError(sysSvcStartExc);
                    }
                    errorLog.logInfo("Started " + serviceName.displayValue + " on " + bindAddressStr + ":" + port);
                }
            }
        }
    }

    private String loadPropChecked(Props props, String key) throws SystemServiceStartException
    {
        String value = null;
        try
        {
            value = props.getProp(key);
            if (value == null)
            {
                throw new SystemServiceStartException(
                    "Missing Property",
                    String.format("The propContainer %s is missing an entry with the key %s",
                        props.getPath(),
                        key
                    ),
                    null,
                    "Insert the necessary property",
                    null);
            }

        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ImplementationError("Constant key is invalid " + key, invalidKeyExc);
        }

        return value;
    }

    /**
     * Generates a random value for a DRBD resource's shared secret
     *
     * @return
     */
    private String generateSharedSecret()
    {
        byte[] randomBytes = new byte[DRBD_SHARED_SECRET_SIZE];
        new SecureRandom().nextBytes(randomBytes);
        String secret = Base64.encode(randomBytes);
        return secret;
    }

    private static class ConnTracker implements ConnectionObserver
    {
        private final Controller controller;

        ConnTracker(Controller controllerRef)
        {
            controller = controllerRef;
        }

        @Override
        public void outboundConnectionEstablished(Peer connPeer)
        {
            if (connPeer != null)
            {
                ControllerPeerCtx peerCtx = (ControllerPeerCtx) connPeer.getAttachment();
                if (peerCtx == null)
                {
                    peerCtx = new ControllerPeerCtx();
                    connPeer.attach(peerCtx);
                }
                synchronized (controller.peerMap)
                {
                    controller.peerMap.put(connPeer.getId(), connPeer);
                }
            }
            // TODO: If a satellite has been connected, schedule any necessary actions
        }

        @Override
        public void inboundConnectionEstablished(Peer connPeer)
        {
            if (connPeer != null)
            {
                ControllerPeerCtx peerCtx = new ControllerPeerCtx();
                connPeer.attach(peerCtx);
                synchronized (controller.peerMap)
                {
                    controller.peerMap.put(connPeer.getId(), connPeer);
                }
            }
        }

        @Override
        public void connectionClosed(Peer connPeer)
        {
            if (connPeer != null)
            {
                synchronized (controller.peerMap)
                {
                    controller.peerMap.remove(connPeer.getId());
                }
            }
        }
    }

    private static class DebugConsoleImpl extends BaseDebugConsole
    {
        private final Controller controller;

        public static final String CONSOLE_PROMPT = "Command ==> ";

        private boolean loadedCmds  = false;
        private boolean exitFlag    = false;

        public static final String[] GNRC_COMMAND_CLASS_LIST =
        {
        };
        public static final String GNRC_COMMAND_CLASS_PKG = "com.linbit.drbdmanage.debug";
        public static final String[] CTRL_COMMAND_CLASS_LIST =
        {
            "CmdDisplayThreads",
            "CmdDisplayContextInfo",
            "CmdDisplayServices",
            "CmdDisplaySecLevel",
            "CmdDisplayModuleInfo",
            "CmdDisplayVersion",
            "CmdStartService",
            "CmdEndService",
            "CmdDisplayConnections",
            "CmdCloseConnection",
            "CmdTestErrorLog",
            "CmdShutdown"
        };
        public static final String CTRL_COMMAND_CLASS_PKG = "com.linbit.drbdmanage.debug";

        private DebugControl debugCtl;

        DebugConsoleImpl(
            Controller controllerRef,
            AccessContext accCtx
        )
        {
            super(accCtx, controllerRef);
            ErrorCheck.ctorNotNull(DebugConsoleImpl.class, Controller.class, controllerRef);
            ErrorCheck.ctorNotNull(DebugConsoleImpl.class, AccessContext.class, accCtx);

            controller = controllerRef;
            loadedCmds = false;
            debugCtl = new DebugControlImpl(controller);
        }

        public void stdStreamsConsole()
        {
            stdStreamsConsole(CONSOLE_PROMPT);
        }

        public void loadDefaultCommands(
            PrintStream debugOut,
            PrintStream debugErr
        )
        {
            if (!loadedCmds)
            {
                for (String cmdClassName : CTRL_COMMAND_CLASS_LIST)
                {
                    loadCommand(debugOut, debugErr, cmdClassName);
                }
            }
            loadedCmds = true;
        }

        @Override
        public void loadCommand(
            PrintStream debugOut,
            PrintStream debugErr,
            String cmdClassName
        )
        {
            try
            {
                Class<? extends Object> cmdClass = Class.forName(
                    CTRL_COMMAND_CLASS_PKG + "." + cmdClassName
                );
                try
                {
                    CommonDebugCmd cmnDebugCmd = (CommonDebugCmd) cmdClass.newInstance();
                    cmnDebugCmd.commonInitialize(controller, controller, debugCtl, this);
                    if (cmnDebugCmd instanceof ControllerDebugCmd)
                    {
                        ControllerDebugCmd debugCmd = (ControllerDebugCmd) cmnDebugCmd;
                        debugCmd.initialize(controller, controller, debugCtl, this);
                    }

                    // FIXME: Detect and report name collisions
                    for (String cmdName : cmnDebugCmd.getCmdNames())
                    {
                        commandMap.put(cmdName.toUpperCase(), cmnDebugCmd);
                    }
                }
                catch (IllegalAccessException | InstantiationException instantiateExc)
                {
                    controller.getErrorReporter().reportError(instantiateExc);
                }
            }
            catch (ClassNotFoundException cnfExc)
            {
                controller.getErrorReporter().reportError(cnfExc);
            }
        }

        @Override
        public void unloadCommand(
            PrintStream debugOut,
            PrintStream debugErr,
            String cmdClassName
        )
        {
            // TODO: Implement
        }
    }

    public interface DebugControl extends CommonDebugControl
    {
    }

    private static class DebugControlImpl implements DebugControl
    {
        Controller controller;

        DebugControlImpl(Controller controllerRef)
        {
            controller = controllerRef;
        }

        @Override
        public String getProgramName()
        {
            return PROGRAM;
        }

        @Override
        public String getModuleType()
        {
            return MODULE;
        }

        @Override
        public String getVersion()
        {
            return VERSION;
        }

        @Override
        public Map<ServiceName, SystemService> getSystemServiceMap()
        {
            Map<ServiceName, SystemService> svcCpy = new TreeMap<>();
            svcCpy.putAll(controller.systemServicesMap);
            return svcCpy;
        }

        @Override
        public Peer getPeer(String peerId)
        {
            Peer peerObj = null;
            synchronized (controller.peerMap)
            {
                peerObj = controller.peerMap.get(peerId);
            }
            return peerObj;
        }

        @Override
        public Map<String, Peer> getAllPeers()
        {
            TreeMap<String, Peer> peerMapCpy = new TreeMap<>();
            synchronized (controller.peerMap)
            {
                peerMapCpy.putAll(controller.peerMap);
            }
            return peerMapCpy;
        }

        @Override
        public void shutdown(AccessContext accCtx)
        {
            try
            {
                controller.shutdown(accCtx);
            }
            catch (AccessDeniedException accExc)
            {
                controller.getErrorReporter().reportError(accExc);
            }
        }
    }
}
