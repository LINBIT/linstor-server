package com.linbit.drbdmanage;

import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.WorkerPool;
import com.linbit.drbdmanage.dbdrivers.NoOpDriver;
import com.linbit.drbdmanage.debug.BaseDebugConsole;
import com.linbit.drbdmanage.debug.CommonDebugCmd;
import com.linbit.drbdmanage.debug.DebugConsole;
import com.linbit.drbdmanage.debug.SatelliteDebugCmd;
import com.linbit.drbdmanage.logging.StdErrorReporter;
import com.linbit.drbdmanage.netcom.ConnectionObserver;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnector;
import com.linbit.drbdmanage.netcom.TcpConnectorService;
import com.linbit.drbdmanage.netcom.ssl.SslTcpConnectorService;
import com.linbit.drbdmanage.proto.CommonMessageProcessor;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.EmptySecurityDbDriver;
import com.linbit.drbdmanage.security.Initializer;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.Privilege;
import com.linbit.drbdmanage.timer.CoreTimer;
import com.linbit.fsevent.FileSystemWatch;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.event.Level;

/**
 * drbdmanageNG satellite prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Satellite extends DrbdManage implements Runnable, SatelliteCoreServices
{
    // System module information
    public static final String MODULE = "Satellite";

    // TCP Service configuration file
    public static final String NET_COM_CONF_FILE = "satellite_netcom.cfg";
    // Plain TCP Service configuration keys
    public static final String NET_COM_CONF_TYPE_KEY = "type";
    public static final String NET_COM_CONF_BIND_ADDR_KEY = "bind-address";
    public static final String NET_COM_CONF_PORT_KEY = "port";
    public static final String NET_COM_CONF_TYPE_PLAIN = "plain";
    public static final String NET_COM_CONF_TYPE_SSL = "ssl";
    // SSL Service configuration keys
    public static final String NET_COM_CONF_SSL_SERVER_CERT_KEY = "server-certificate";
    public static final String NET_COM_CONF_SSL_TRUST_CERT_KEY = "trusted-certificates";
    public static final String NET_COM_CONF_SSL_KEY_PASS_KEY = "key-passwd";
    public static final String NET_COM_CONF_SSL_KEYSTORE_PASS_KEY = "keystore-passwd";
    public static final String NET_COM_CONF_SSL_TRUST_PASS_KEY = "truststore-passwd";
    public static final String NET_COM_CONF_SSL_CONTEXT_TYPE_KEY = "ssl-context-type";

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

    // ============================================================
    // Core system services
    //
    // Map of controllable system services
    private final Map<ServiceName, SystemService> systemServicesMap;

    // Map of connected peers
    private final Map<String, Peer> peerMap;

    // Map of network communications connectors
    private final Map<ServiceName, TcpConnector> netComConnectors;

    // File system watch service
    private FileSystemWatch fsWatchSvc;

    // Shutdown controls
    private boolean shutdownFinished;
    private ObjectProtection shutdownProt;

    // Lock for major global changes
    private final ReadWriteLock reconfigurationLock;

    public Satellite(AccessContext sysCtxRef, AccessContext publicCtxRef, String[] argsRef)
        throws IOException
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
        fsWatchSvc = new FileSystemWatch();
        systemServicesMap.put(fsWatchSvc.getInstanceName(), fsWatchSvc);

        // Initialize network communications connectors map
        netComConnectors = new TreeMap<>();

        // Initialize connected peers map
        peerMap = new TreeMap<>();

        // Initialize shutdown controls
        shutdownFinished = false;
        try
        {
            shutdownProt = ObjectProtection.create(
                ObjectProtection.buildPath(this, "shutdown"),
                sysCtx,
                null
            );
        }
        catch (SQLException sqlExc)
        {
            // cannot happen
            throw new ImplementationError(
                "Creating an ObjectProtection without TransactionManager threw an SQLException",
                sqlExc
            );
        }
    }

    public void initialize(ErrorReporter errorLogRef)
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
                try
                {
                    shutdownProt.addAclEntry(initCtx, sysCtx.getRole(), AccessType.CONTROL);
                }
                catch (SQLException sqlExc)
                {
                    // cannot happen
                    throw new ImplementationError(
                        "ObjectProtection without TransactionManager threw an SQLException",
                        sqlExc
                    );
                }

                securityDbDriver = new EmptySecurityDbDriver();
                persistenceDbDriver = new NoOpDriver();

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

                // Initialize system services
                startSystemServices(systemServicesMap.values());

                // Initialize the network communications service
                errorLogRef.logInfo("Initializing main network communications service");
                initMainNetComService();
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
        Satellite.DebugConsoleImpl peerDbgConsole = new Satellite.DebugConsoleImpl(this, debugCtx);
        if (client != null)
        {
            SatellitePeerCtx peerContext = (SatellitePeerCtx) client.getAttachment();
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

    @Override
    public FileSystemWatch getFsWatch()
    {
        return fsWatchSvc;
    }

    private void initMainNetComService()
    {
        try
        {
            Properties netComProps = new Properties();
            try (InputStream propsIn = new FileInputStream(NET_COM_CONF_FILE))
            {
                netComProps.loadFromXML(propsIn);
            }

            InetAddress addr = InetAddress.getByName(netComProps.getProperty(NET_COM_CONF_BIND_ADDR_KEY));
            String portProp = netComProps.getProperty(NET_COM_CONF_PORT_KEY);
            int port = Integer.parseInt(portProp);
            SocketAddress bindAddress = new InetSocketAddress(addr, port);

            TcpConnector netComSvc = null;

            String type = netComProps.getProperty(NET_COM_CONF_TYPE_KEY, "");
            if (type.equalsIgnoreCase(NET_COM_CONF_TYPE_PLAIN))
            {
                netComSvc = new TcpConnectorService(
                    this,
                    msgProc,
                    bindAddress,
                    publicCtx,
                    new ConnTracker(this)
                );
            }
            else
            if (type.equalsIgnoreCase(NET_COM_CONF_TYPE_SSL))
            {
                String sslProtocol = netComProps.getProperty(NET_COM_CONF_SSL_CONTEXT_TYPE_KEY);
                String keyStoreFile = netComProps.getProperty(NET_COM_CONF_SSL_SERVER_CERT_KEY);
                String trustStoreFile = netComProps.getProperty(NET_COM_CONF_SSL_TRUST_CERT_KEY);
                char[] keyPasswd = netComProps.getProperty(NET_COM_CONF_SSL_KEY_PASS_KEY).toCharArray();
                char[] keyStorePasswd = netComProps.getProperty(NET_COM_CONF_SSL_KEYSTORE_PASS_KEY).toCharArray();
                char[] trustStorePasswd = netComProps.getProperty(NET_COM_CONF_SSL_TRUST_PASS_KEY).toCharArray();

                try
                {
                    netComSvc = new SslTcpConnectorService(
                        this,
                        msgProc,
                        bindAddress,
                        publicCtx,
                        new ConnTracker(this),
                        sslProtocol,
                        keyStoreFile,
                        keyStorePasswd,
                        keyPasswd,
                        trustStoreFile,
                        trustStorePasswd
                    );
                }
                catch (KeyManagementException keyMgmtExc)
                {
                    getErrorReporter().reportProblem(
                        Level.ERROR,
                        new DrbdManageException(
                            "Initialization of the SSLContext failed. See cause for details",
                            keyMgmtExc
                        ),
                        null, // accCtx
                        null, // client
                        null  // contextInfo
                    );
                }
                catch (UnrecoverableKeyException unrecoverableKeyExc)
                {
                    String errorMsg = "A private or public key for the initialization of SSL encryption could " +
                        "not be loaded";
                    getErrorReporter().reportProblem(
                        Level.ERROR,
                        new DrbdManageException(
                            errorMsg,
                            errorMsg,
                            null,
                            "Check whether the password for the SSL keystores is correct.",
                            null,
                            unrecoverableKeyExc),
                        null, // accCtx
                        null, // client
                        null  // contextInfo
                    );
                }
                catch (NoSuchAlgorithmException exc)
                {
                    getErrorReporter().reportProblem(
                        Level.ERROR,
                        new DrbdManageException(
                            String.format(
                                "SSL initialization failed: " +
                                "The SSL/TLS encryption protocol '%s' is not available on this system.",
                                sslProtocol
                            ),
                            "SSL initialization failed.",
                            String.format(
                                "The SSL/TLS protocol '%s' is not available on this system",
                                sslProtocol
                            ),
                            "- Select a supported SSL/TLS protocol in the network communications configuration\n" +
                            "or\n" +
                            "- Enable support for the currently selected SSL/TLS protocol on this system",
                            null,
                            exc
                        ),
                        null, // accCtx
                        null, // client
                        null  //contextInfo
                    );
                }
                catch (KeyStoreException keyStoreExc)
                {
                    throw new ImplementationError(
                        "Default SSL keystore type could not be found by the KeyStore instance",
                        keyStoreExc
                    );
                }
                catch (CertificateException exc)
                {
                    getErrorReporter().reportProblem(
                        Level.ERROR,
                        new DrbdManageException(
                            "A required SSL certificate could not be loaded",
                            "A required SSL certificate could not be loaded from the keystore files",
                            null,
                            "Ensure that the required SSL certificates are contained in the keystore files.\n" +
                            "Refer to documentation for information on how to setup SSL encryption.",
                            null,
                            exc
                        ),
                        null, // accCtx
                        null, // client
                        null  // contextInfo
                    );
                }
            }

            if (netComSvc != null)
            {
                try
                {
                    netComSvc.setServiceInstanceName(new ServiceName("InitialConnector"));
                    netComConnectors.put(netComSvc.getInstanceName(), netComSvc);
                    systemServicesMap.put(netComSvc.getInstanceName(), netComSvc);
                    netComSvc.start();
                }
                catch (SystemServiceStartException sysSvcStartExc )
                {
                    String errorMsg = sysSvcStartExc.getMessage();
                    if (errorMsg == null)
                    {
                        errorMsg = "The initial network communications service failed to start.";
                    }
                    getErrorReporter().reportProblem(
                        Level.ERROR,
                        new DrbdManageException(
                            errorMsg,
                            errorMsg, // description
                            null, // cause
                            null, // correction
                            null, // details
                            sysSvcStartExc // Nested throwable
                        ),
                        null, // accessContext
                        null, // client
                        null  // contextInfo
                    );

                }
                catch (InvalidNameException invalidNameExc)
                {
                    throw new ImplementationError(
                        "Hardcoded ServiceInstanceName for netComSvc is invalid",
                        invalidNameExc
                    );
                }
            }
            else
            {
                getErrorReporter().reportProblem(
                    Level.ERROR,
                    new DrbdManageException(
                        // Message
                        String.format(
                            "The property '%s' in configuration file '%s' is misconfigured",
                            NET_COM_CONF_TYPE_KEY, NET_COM_CONF_FILE
                        ),
                        // Description
                        "The initial network communication service can not be started.",
                        // Cause
                        String.format(
                            "The service type is misconfigured.\n" +
                            "The property '%s' must be either '%s' or '%s",
                            NET_COM_CONF_TYPE_KEY,
                            NET_COM_CONF_TYPE_PLAIN,
                            NET_COM_CONF_TYPE_SSL
                        ),
                        // Error details
                        String.format(
                            "The network communication service configuration file is:\n%s",
                            NET_COM_CONF_FILE
                        ),
                        // No nested exception
                        null
                    ),
                    // No access context
                    null,
                    // Not initiated by a connected client
                    null,
                    // No context information
                    null
                );
            }
        }
        catch (FileNotFoundException fileExc)
        {
            String errorMsg = "The configuration file for the initial network communications service could " +
                "not be opened.";
            String fileExcMsg = fileExc.getMessage();
            getErrorReporter().reportProblem(
                Level.ERROR,
                new DrbdManageException(
                    fileExcMsg != null ?
                        "Can not open configuration file: " + fileExc.getMessage() :
                        errorMsg + " The system did not provide any error description.",
                    "The configuration file for the initial network communications service could not be opened.",
                    fileExcMsg != null ?
                        "The system returned the following error description:\n" +
                        fileExc.getMessage() :
                        "The system did not provide any error description.",
                    "Make sure that the file exists and can be accessed by this process.",
                    null
                ),
                null, // No access context
                null, // No client connection
                null // No context information
            );
        }
        catch (IOException ioExc)
        {
            getErrorReporter().reportError(ioExc);
        }
    }

    public static void main(String[] args)
    {
        System.out.printf(
            "%s, Module %s, Release %s\n",
            Satellite.PROGRAM, Satellite.MODULE, Satellite.VERSION
        );
        printStartupInfo();

        ErrorReporter errorLog = new StdErrorReporter(Satellite.MODULE);

        try
        {
            Thread.currentThread().setName("Main");

            // Initialize the Satellite module with the SYSTEM security context
            Initializer sysInit = new Initializer();
            Satellite instance = sysInit.initSatellite(args);

            instance.initialize(errorLog);
            instance.run();
        }
        catch (ImplementationError implError)
        {
            errorLog.reportError(implError);
        }
        catch (IOException ioExc)
        {
            errorLog.reportError(ioExc);
        }
        catch (Throwable error)
        {
            errorLog.reportError(error);
        }

        System.out.println();
    }

    private static class ConnTracker implements ConnectionObserver
    {
        private final Satellite satellite;

        ConnTracker(Satellite satelliteRef)
        {
            satellite = satelliteRef;
        }

        @Override
        public void outboundConnectionEstablished(Peer connPeer)
        {
            // FIXME: Something should done here for completeness, although the Satellite
            //        does not normally connect outbound
            if (connPeer != null)
            {
                SatellitePeerCtx peerCtx = (SatellitePeerCtx) connPeer.getAttachment();
                if (peerCtx == null)
                {
                    peerCtx = new SatellitePeerCtx();
                    connPeer.attach(peerCtx);
                }
                synchronized (satellite.peerMap)
                {
                    satellite.peerMap.put(connPeer.getId(), connPeer);
                }
            }
        }

        @Override
        public void inboundConnectionEstablished(Peer connPeer)
        {
            if (connPeer != null)
            {
                SatellitePeerCtx peerCtx = new SatellitePeerCtx();
                connPeer.attach(peerCtx);
                synchronized (satellite.peerMap)
                {
                    satellite.peerMap.put(connPeer.getId(), connPeer);
                }
            }
        }

        @Override
        public void connectionClosed(Peer connPeer)
        {
            if (connPeer != null)
            {
                synchronized (satellite.peerMap)
                {
                    satellite.peerMap.remove(connPeer.getId());
                }
            }
        }
    }

    private static class DebugConsoleImpl extends BaseDebugConsole
    {
        private final Satellite satellite;

        private boolean loadedCmds  = false;
        private boolean exitFlag    = false;

        public static final String CONSOLE_PROMPT = "Command ==> ";

        public static final String[] GNRC_COMMAND_CLASS_LIST =
        {
        };
        public static final String GNRC_COMMAND_CLASS_PKG = "com.linbit.drbdmanage.debug";
        public static final String[] STLT_COMMAND_CLASS_LIST =
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
            "CmdDisplaySystemStatus",
            "CmdDisplayApis",
            "CmdTestErrorLog",
            "CmdShutdown"
        };
        public static final String STLT_COMMAND_CLASS_PKG = "com.linbit.drbdmanage.debug";

        private DebugControl debugCtl;

        DebugConsoleImpl(
            Satellite satelliteRef,
            AccessContext accCtx
        )
        {
            super(accCtx, satelliteRef);
            ErrorCheck.ctorNotNull(DebugConsoleImpl.class, Satellite.class, satelliteRef);
            ErrorCheck.ctorNotNull(DebugConsoleImpl.class, AccessContext.class, accCtx);

            satellite = satelliteRef;
            loadedCmds = false;
            debugCtl = new DebugControlImpl(satellite);
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
                for (String cmdClassName : STLT_COMMAND_CLASS_LIST)
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
                    STLT_COMMAND_CLASS_PKG + "." + cmdClassName
                );
                try
                {
                    CommonDebugCmd cmnDebugCmd = (CommonDebugCmd) cmdClass.newInstance();
                    cmnDebugCmd.commonInitialize(satellite, satellite, debugCtl, this);
                    if (cmnDebugCmd instanceof SatelliteDebugCmd)
                    {
                        SatelliteDebugCmd debugCmd = (SatelliteDebugCmd) cmnDebugCmd;
                        debugCmd.initialize(satellite, satellite, debugCtl, this);
                    }

                    // FIXME: Detect and report name collisions
                    for (String cmdName : cmnDebugCmd.getCmdNames())
                    {
                        commandMap.put(cmdName.toUpperCase(), cmnDebugCmd);
                    }
                }
                catch (IllegalAccessException | InstantiationException instantiateExc)
                {
                    satellite.getErrorReporter().reportError(instantiateExc);
                }
            }
            catch (ClassNotFoundException cnfExc)
            {
                satellite.getErrorReporter().reportError(cnfExc);
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

    public static interface DebugControl extends CommonDebugControl
    {
        Satellite getModuleInstance();
    }

    private static class DebugControlImpl implements DebugControl
    {
        Satellite satellite;

        DebugControlImpl(Satellite satelliteRef)
        {
            satellite = satelliteRef;
        }

        @Override
        public DrbdManage getInstance()
        {
            return satellite;
        }

        @Override
        public Satellite getModuleInstance()
        {
            return satellite;
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
            svcCpy.putAll(satellite.systemServicesMap);
            return svcCpy;
        }

        @Override
        public Peer getPeer(String peerId)
        {
            Peer peerObj = null;
            synchronized (satellite.peerMap)
            {
                peerObj = satellite.peerMap.get(peerId);
            }
            return peerObj;
        }

        @Override
        public Map<String, Peer> getAllPeers()
        {
            TreeMap<String, Peer> peerMapCpy = new TreeMap<>();
            synchronized (satellite.peerMap)
            {
                peerMapCpy.putAll(satellite.peerMap);
            }
            return peerMapCpy;
        }

        @Override
        public Set<String> getApiCallNames()
        {
            return satellite.msgProc.getApiCallNames();
        }

        @Override
        public void shutdown(AccessContext accCtx)
        {
            try
            {
                satellite.shutdown(accCtx);
            }
            catch (AccessDeniedException accExc)
            {
                satellite.getErrorReporter().reportError(accExc);
            }
        }
    }
}
