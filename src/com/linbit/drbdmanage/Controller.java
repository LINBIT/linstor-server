package com.linbit.drbdmanage;

import com.linbit.*;
import com.linbit.drbdmanage.controllerapi.*;
import com.linbit.drbdmanage.debug.*;
import com.linbit.drbdmanage.netcom.ConnectionObserver;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnectorService;
import com.linbit.drbdmanage.netcom.ssl.SslTcpConnectorService;
import com.linbit.drbdmanage.netcom.ssl.SslTcpConstants;
import com.linbit.drbdmanage.proto.CommonMessageProcessor;
import com.linbit.drbdmanage.security.*;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.timer.Action;
import com.linbit.timer.GenericTimer;
import com.linbit.timer.Timer;

import java.io.IOException;
import java.io.PrintStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * drbdmanageNG controller prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class Controller implements Runnable, CoreServices
{
    public static final String PROGRAM = "drbdmanageNG";
    public static final String MODULE = "Controller";
    public static final String VERSION = "experimental 2017-03-14_001";

    public static final int MIN_WORKER_QUEUE_SIZE = 32;
    public static final int MAX_CPU_COUNT = 1024;

    // At shutdown, wait at most SHUTDOWN_THR_JOIN_WAIT milliseconds for
    // a service thread to end
    public static final long SHUTDOWN_THR_JOIN_WAIT = 3000L;

    public static final IdentityName ID_ANON_CLIENT_NAME;
    public static final RoleName ROLE_PUBLIC_NAME;
    public static final SecTypeName TYPE_PUBLIC_NAME;

    // Defaults
    private int cpuCount = 8;
    private int workerThreadCount = 8;
    // Queue slots per worker thread
    private int workerQueueFactor = 4;
    private int workerQueueSize = MIN_WORKER_QUEUE_SIZE;

    public static final String SCREEN_DIV =
        "------------------------------------------------------------------------------";

    // System security context
    private final AccessContext sysCtx;
    // Default security identity for unauthenticated clients
    private final Identity      anonClientId;
    // Public access security role
    private final Role          publicRole;
    // Public access security type
    private final SecurityType  publicType;

    private String[] args;

    private final GenericTimer<String, Action<String>> timerEventSvc;
    private final FileSystemWatch fsEventSvc;

    // Map of controllable system services
    private final Map<ServiceName, SystemService> systemServicesMap;

    // Map of connected peers
    private final Map<String, Peer> peerMap;

    private WorkerPool workers = null;
    private ErrorReporter errorLog = null;

    private TcpConnectorService netComSvc = null;
    private CommonMessageProcessor msgProc = null;

    private boolean shutdownFinished;
    private ObjectProtection shutdownProt;

    static
    {
        try
        {
            ID_ANON_CLIENT_NAME = new IdentityName("AnonymousClient");
            ROLE_PUBLIC_NAME    = new RoleName("Public");
            TYPE_PUBLIC_NAME    = new SecTypeName("Public");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The " + Controller.class.getName() + " class contains invalid " +
                "security object name constants",
                nameExc
            );
        }
    }

    public Controller(AccessContext sysCtxRef, String[] argsRef)
        throws IOException
    {
        sysCtx = sysCtxRef;
        args = argsRef;

        // Initialize security objects
        {
            AccessContext initCtx = sysCtx.clone();
            try
            {
                initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                anonClientId = new Identity(initCtx, ID_ANON_CLIENT_NAME);
                publicRole = new Role(initCtx, ROLE_PUBLIC_NAME);
                publicType = new SecurityType(initCtx, TYPE_PUBLIC_NAME);
            }
            catch (AccessDeniedException accessExc)
            {
                throw new ImplementationError(
                    "Controller initialization failed: Initialization of security contexts failed",
                    accessExc
                );
            }
        }

        // Create the timer event service
        timerEventSvc = new GenericTimer<>();

        // Create the filesystem event service
        try
        {
            fsEventSvc = new FileSystemWatch();
        }
        catch (IOException ioExc)
        {
            logFailure("Initialization of the FileSystemWatch service failed");
            // FIXME: Generate a startup exception
            throw ioExc;
        }

        systemServicesMap = new TreeMap<>();
        systemServicesMap.put(timerEventSvc.getInstanceName(), timerEventSvc);
        systemServicesMap.put(fsEventSvc.getInstanceName(), fsEventSvc);

        peerMap = new TreeMap<>();

        cpuCount = Runtime.getRuntime().availableProcessors();

        shutdownFinished = false;
        shutdownProt = new ObjectProtection(sysCtx);
    }

    @Override
    public void run()
    {
        try
        {
            logInfo("Entering debug console");
            AccessContext debugCtx;
            {
                AccessContext impCtx = sysCtx.clone();
                impCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                debugCtx = impCtx.impersonate(
                    new Identity(impCtx, new IdentityName("LocalDebugConsole")),
                    sysCtx.subjectRole,
                    sysCtx.subjectDomain,
                    sysCtx.getLimitPrivs().toArray()
                );
            }
            debugCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_OBJ_VIEW);
            DebugConsoleImpl dbgConsoleInstance = new DebugConsoleImpl(this, debugCtx);
            dbgConsoleInstance.loadDefaultCommands(System.out, System.err);
            DebugConsoleImpl dbgConsole = dbgConsoleInstance;
            dbgConsole.stdStreamsConsole();
            System.out.println();
        }
        catch (Throwable error)
        {
            errorLog.reportError(error);
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
                null
            );
        }
    }

    public void initialize(ErrorReporter errorLogRef, SSLConfiguration sslConfig) throws KeyManagementException, UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, CertificateException
    {
        shutdownFinished = false;

        errorLog = errorLogRef;

        System.out.printf("\n%s\n\n", SCREEN_DIV);
        programInfo();
        System.out.printf("\n%s\n\n", SCREEN_DIV);

        logInit("Applying base security policy to system objects");
        applyBaseSecurityPolicy();

        logInit("Starting worker thread pool");
        workerThreadCount = cpuCount <= MAX_CPU_COUNT ? cpuCount : MAX_CPU_COUNT;
        {
            int qSize = workerThreadCount * workerQueueFactor;
            workerQueueSize = qSize > MIN_WORKER_QUEUE_SIZE ? qSize : MIN_WORKER_QUEUE_SIZE;
        }
        workers = WorkerPool.initialize(workerThreadCount, workerQueueSize, true, "MainWorkerPool", errorLog);

        // Initialize the message processor
        logInit("Initializing API call dispatcher");
        msgProc = new CommonMessageProcessor(this, workers);

        logInit("Initializing test APIs");
        {
            msgProc.addApiCall(new Ping(this, this));
            msgProc.addApiCall(new CreateDebugConsole(this, this));
            msgProc.addApiCall(new DestroyDebugConsole(this, this));
            msgProc.addApiCall(new DebugCommand(this, this));
            msgProc.addApiCall(new DebugMakeSuperuser(this, this));
        }

        // Initialize the network communications service
        logInit("Initializing network communications service");
        initializeNetComService(sslConfig);

        // Start service threads
        for (SystemService sysSvc : systemServicesMap.values())
        {
            logInfo(
                String.format(
                    "Starting service instance '%s' of type %s",
                    sysSvc.getInstanceName().displayValue, sysSvc.getServiceName().displayValue
                )
            );
            try
            {
                sysSvc.start();
            }
            catch (SystemServiceStartException startExc)
            {
                errorLog.reportError(startExc);
                logFailure(
                    String.format(
                        "Start of the service instance '%s' of type %s failed",
                        sysSvc.getInstanceName().displayValue, sysSvc.getServiceName().displayValue
                    )
                );
            }
        }


        System.out.printf("\n%s\n\n", SCREEN_DIV);
        runTimeInfo();
        System.out.printf("\n%s\n\n", SCREEN_DIV);
    }

    public void shutdown(AccessContext accCtx) throws AccessDeniedException
    {
        shutdownProt.requireAccess(accCtx, AccessType.USE);
        if (!shutdownFinished)
        {
            logInfo(
                String.format(
                    "Shutdown initiated by subject '%s' using role '%s'\n",
                    accCtx.getIdentity(), accCtx.getRole()
                )
            );

            logInfo("Shutdown in progress");

            // Shutdown service threads
            for (SystemService sysSvc : systemServicesMap.values())
            {
                logInfo(
                    String.format(
                        "Shutting down service instance '%s' of type %s",
                        sysSvc.getInstanceName().displayValue, sysSvc.getServiceName().displayValue
                    )
                );
                sysSvc.shutdown();

                logInfo(
                    String.format(
                        "Waiting for service instance '%s' to complete shutdown",
                        sysSvc.getInstanceName().displayValue
                    )
                );
                try
                {
                    sysSvc.awaitShutdown(SHUTDOWN_THR_JOIN_WAIT);
                }
                catch (InterruptedException intrExc)
                {
                    errorLog.reportError(intrExc);
                }
            }

            if (workers != null)
            {
                logInfo("Shutting down worker thread pool");
                workers.shutdown();
                workers = null;
            }

            logInfo("Shutdown complete");
        }
        shutdownFinished = true;
    }

    public DebugConsole createDebugConsole(AccessContext accCtx, Peer client)
        throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        CtlPeerContext peerContext = (CtlPeerContext) client.getAttachment();
        DebugConsoleImpl peerDbgConsole = new DebugConsoleImpl(this, accCtx);
        peerDbgConsole.loadDefaultCommands(System.out, System.err);
        peerContext.setDebugConsole(peerDbgConsole);

        return new DebugConsoleImpl(this, accCtx);
    }

    public void destroyDebugConsole(AccessContext accCtx, Peer client)
        throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        CtlPeerContext peerContext = (CtlPeerContext) client.getAttachment();
        peerContext.setDebugConsole(null);
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
                new Identity(impCtx, new IdentityName("DebugClient")),
                sysCtx.getRole(),
                sysCtx.getDomain(),
                Privilege.PRIV_SYS_ALL
            );
            client.setAccessContext(impCtx, privPeerCtx);
            successFlag = true;
        }
        catch (InvalidNameException nameExc)
        {
            errorLog.reportError(nameExc);
        }
        catch (AccessDeniedException accessExc)
        {
            errorLog.reportError(accessExc);
        }
        return successFlag;
    }

    @Override
    public ErrorReporter getErrorReporter()
    {
        return errorLog;
    }

    @Override
    public Timer<String, Action<String>> getTimer()
    {
        return timerEventSvc;
    }

    @Override
    public FileSystemWatch getFsWatch()
    {
        return fsEventSvc;
    }

    public static final void logInit(String what)
    {
        System.out.println("INIT      " + what);
    }

    public static final void logInfo(String what)
    {
        System.out.println("INFO      " + what);
    }

    public static final void logBegin(String what)
    {
        System.out.println("BEGIN     " + what);
    }

    public static final void logEnd(String what)
    {
        System.out.println("END       " + what);
    }

    public static final void logFailure(String what)
    {
        System.err.println("FAILED    " + what);
    }

    public static final void printField(String fieldName, String fieldContent)
    {
        System.out.printf("  %-32s: %s\n", fieldName, fieldContent);
    }

    public final void programInfo()
    {
        System.out.println(
            "Software information\n" +
            "--------------------\n"
        );

        printField("PROGRAM", PROGRAM);
        printField("MODULE", MODULE);
        printField("VERSION", VERSION);
    }

    public final void runTimeInfo()
    {
        Properties sysProps = System.getProperties();
        String jvmSpecVersion = sysProps.getProperty("java.vm.specification.version");
        String jvmVendor = sysProps.getProperty("java.vm.vendor");
        String jvmVersion = sysProps.getProperty("java.vm.version");
        String osName = sysProps.getProperty("os.name");
        String osVersion = sysProps.getProperty("os.version");
        String sysArch = sysProps.getProperty("os.arch");

        System.out.println(
            "Execution environment information\n" +
            "--------------------------------\n"
        );

        Runtime rt = Runtime.getRuntime();
        long freeMem = rt.freeMemory() / 1048576;
        long availMem = rt.maxMemory() / 1048576;

        printField("JAVA PLATFORM", jvmSpecVersion);
        printField("RUNTIME IMPLEMENTATION", jvmVendor + ", Version " + jvmVersion);
        System.out.println();
        printField("SYSTEM ARCHITECTURE", sysArch);
        printField("OPERATING SYSTEM", osName + " " + osVersion);
        printField("AVAILABLE PROCESSORS", Integer.toString(cpuCount));
        if (availMem == Long.MAX_VALUE)
        {
            printField("AVAILABLE MEMORY", "OS ALLOCATION LIMIT");
        }
        else
        {
            printField("AVAILABLE MEMORY", String.format("%10d MiB", availMem));
        }
        printField("FREE MEMORY", String.format("%10d MiB", freeMem));
        System.out.println();
        printField("WORKER THREADS", Integer.toString(workers.getThreadCount()));
        printField("WORKER QUEUE SIZE", Integer.toString(workers.getQueueSize()));
        printField("WORKER SCHEDULING", workers.isFairQueue() ? "FIFO" : "Random");
    }

    private void applyBaseSecurityPolicy()
    {
        PrivilegeSet effPriv = sysCtx.getEffectivePrivs();
        try
        {
            // Enable all privileges
            effPriv.enablePrivileges(Privilege.PRIV_SYS_ALL);

            // Allow CONTROL access by domain SYSTEM to type SYSTEM
            SecurityType sysType = sysCtx.getDomain();
            sysType.addEntry(sysCtx, sysType, AccessType.CONTROL);

            // Allow USE access by role SYSTEM to shutdownProt
            shutdownProt.addAclEntry(sysCtx, sysCtx.getRole(), AccessType.USE);
        }
        catch (AccessDeniedException accExc)
        {
            logFailure("Applying the base security policy failed");
            errorLog.reportError(accExc);
        }
        finally
        {
            effPriv.disablePrivileges(Privilege.PRIV_SYS_ALL);
        }
    }

    public static void main(String[] args)
    {
        logInit("System components initialization in progress");

        logInit("Constructing error reporter instance");
        ErrorReporter errorLog = new DebugErrorReporter(System.err);

        try
        {
            logInit("Initializing system security context");
            Initializer sysInit = new Initializer();
            logInit("Constructing controller instance");
            Controller instance = sysInit.initController(args);

            logInit("Initializing controller services");
            SSLConfiguration sslConfig = new SSLConfiguration();
            {
                char[] passwd = new char[]
                {
                    'c','h','a','n','g','e','m','e'
                };
                sslConfig.sslProtocol = SslTcpConstants.SSL_CONTEXT_DEFAULT_TYPE;
                sslConfig.keyStoreFile = "keys/ServerKeyStore.jks";
                sslConfig.keyStorePasswd = passwd;
                sslConfig.keyPasswd = passwd;
                sslConfig.trustStoreFile = "keys/TrustStore.jks";
                sslConfig.trustStorePasswd = passwd;
            } // TODO change the ssl config


            instance.initialize(errorLog, sslConfig);

//            {
//                // FOR TESTING PURPOSSES ONLY
//                instance.initialize(errorLog, sslConfig);
//                InetSocketAddress address = new InetSocketAddress("localhost", 9504);
//                Peer peer = instance.netComSvc.connect(address);
//                Message msg = peer.createMessage();
//                msg.setData("Hallo Welt\n".getBytes());
//                peer.waitUntilConnectionEstablished();
//                peer.sendMessage(msg);
//                Thread.sleep(2000);
//                peer.closeConnection();
//            }



            logInit("Initialization complete");
            System.out.println();

            Thread.currentThread().setName("MainLoop");

            logInfo("Starting controller module");
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

    private void initializeNetComService(SSLConfiguration sslConfig) throws KeyManagementException, UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, CertificateException
    {
        try
        {
            AccessContext defaultPeerAccCtx;
            {
                AccessContext impCtx = sysCtx.clone();
                impCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                defaultPeerAccCtx = impCtx.impersonate(
                    anonClientId,
                    publicRole,
                    publicType
                );
            }
            if (sslConfig == null)
            {
                netComSvc = new TcpConnectorService(this, msgProc, defaultPeerAccCtx, new ConnTracker(this));
            }
            else
            {
                netComSvc = new SslTcpConnectorService(
                    this,
                    msgProc,
                    defaultPeerAccCtx,
                    new ConnTracker(this),
                    sslConfig.sslProtocol,
                    sslConfig.keyStoreFile,
                    sslConfig.keyStorePasswd,
                    sslConfig.keyPasswd,
                    sslConfig.trustStoreFile,
                    sslConfig.trustStorePasswd
                );
            }
            systemServicesMap.put(netComSvc.getInstanceName(), netComSvc);
        }
        catch (AccessDeniedException accessExc)
        {
            errorLog.reportError(accessExc);
            logFailure("Cannot create security context for the network communications service");
        }
        catch (IOException exc)
        {
            errorLog.reportError(exc);
        }
    }

    private class ConnTracker implements ConnectionObserver
    {
        private Controller controller;

        ConnTracker(Controller controllerRef)
        {
            controller = controllerRef;
        }

        @Override
        public void outboundConnectionEstablished(Peer connPeer)
        {
            // TODO: Satellite or utility connection connected
        }

        @Override
        public void inboundConnectionEstablished(Peer connPeer)
        {
            if (connPeer != null)
            {
                CtlPeerContext peerCtx = new CtlPeerContext();
                connPeer.attach(peerCtx);
                peerMap.put(connPeer.getId(), connPeer);
            }
        }

        @Override
        public void connectionClosed(Peer connPeer)
        {
            if (connPeer != null)
            {
                peerMap.remove(connPeer.getId());
            }
        }
    }

    private static class DebugConsoleImpl extends BaseDebugConsole
    {
        private final Controller controller;

        public static final String CONSOLE_PROMPT = "Command ==> ";

        private boolean loadedCmds  = false;
        private boolean exitFlag    = false;

        public static final String[] COMMAND_CLASS_LIST =
        {
            "CmdDisplayThreads",
            "CmdDisplayContextInfo",
            "CmdDisplayServices",
            "CmdDisplaySecLevel",
            "CmdStartService",
            "CmdEndService",
            "CmdDisplayConnections",
            "CmdCloseConnection",
            "CmdTestErrorLog",
            "CmdShutdown"
        };
        public static final String COMMAND_CLASS_PKG = "com.linbit.drbdmanage.debug";

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
            parameters = new TreeMap<>();
            commandMap = new TreeMap<>();
            loadedCmds = false;
            debugCtl = new DebugControlImpl(controller);
        }

        public void stdStreamsConsole()
        {
            stdStreamsConsole(CONSOLE_PROMPT);
        }

        @Override
        public Map<String, CommonDebugCmd> getCommandMap()
        {
            return commandMap;
        }

        public void loadDefaultCommands(
            PrintStream debugOut,
            PrintStream debugErr
        )
        {
            if (!loadedCmds)
            {
                for (String cmdClassName : COMMAND_CLASS_LIST)
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
                    COMMAND_CLASS_PKG + "." + cmdClassName
                );
                try
                {
                    ControllerDebugCmd debugCmd = (ControllerDebugCmd) cmdClass.newInstance();
                    debugCmd.initialize(controller, controller, debugCtl, this);

                    // FIXME: Detect and report name collisions
                    for (String cmdName : debugCmd.getCmdNames())
                    {
                        commandMap.put(cmdName.toUpperCase(), debugCmd);
                    }
                }
                catch (IllegalAccessException | InstantiationException instantiateExc)
                {
                    controller.errorLog.reportError(instantiateExc);
                }
            }
            catch (ClassNotFoundException cnfExc)
            {
                controller.errorLog.reportError(cnfExc);
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

    public interface DebugControl
    {
        Map<ServiceName, SystemService> getSystemServiceMap();
        Peer getPeer(String peerId);
        Map<String, Peer> getAllPeers();
        void shutdown(AccessContext accCtx);
    }

    private static class DebugControlImpl implements DebugControl
    {
        Controller controller;

        DebugControlImpl(Controller controllerRef)
        {
            controller = controllerRef;
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
                controller.errorLog.reportError(accExc);
            }
        }
    }

    public static class SSLConfiguration
    {
        public String sslProtocol;
        public String keyStoreFile;
        public char[] keyStorePasswd;
        public char[] keyPasswd;
        public String trustStoreFile;
        public char[] trustStorePasswd;
    }
}
