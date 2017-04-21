package com.linbit.drbdmanage;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.WorkerPool;
import com.linbit.drbdmanage.debug.BaseDebugConsole;
import com.linbit.drbdmanage.debug.CommonDebugCmd;
import com.linbit.drbdmanage.debug.DebugConsole;
import com.linbit.drbdmanage.debug.DebugErrorReporter;
import com.linbit.drbdmanage.debug.SatelliteDebugCmd;
import com.linbit.drbdmanage.netcom.ConnectionObserver;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnector;
import com.linbit.drbdmanage.proto.CommonMessageProcessor;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.Initializer;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.Privilege;
import com.linbit.drbdmanage.timer.CoreTimer;
import com.linbit.fsevent.FileSystemWatch;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * drbdmanageNG satellite prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Satellite extends DrbdManage implements Runnable, SatelliteCoreServices
{
    // System module information
    public static final String MODULE = "Satellite";

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
        shutdownProt = new ObjectProtection(sysCtx);
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
                shutdownProt.addAclEntry(initCtx, sysCtx.getRole(), AccessType.CONTROL);

                // Initialize the worker thread pool
                logInit("Starting worker thread pool");
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
                logInit("Initializing API call dispatcher");
                msgProc = new CommonMessageProcessor(this, workerThrPool);

                logInit("Initializing test APIs");
                {
                    // TODO: Satellite test APIs
                }

                // Initialize system services
                startSystemServices(systemServicesMap.values(), getErrorReporter());

                // Initialize the network communications service
                logInit("Initializing main network communications service");
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
        try
        {
            logInfo("Entering debug console");

            AccessContext privCtx = sysCtx.clone();
            AccessContext debugCtx = sysCtx.clone();
            privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            DebugConsole dbgConsole = createDebugConsole(privCtx, debugCtx, null);
            dbgConsole.stdStreamsConsole(DebugConsoleImpl.CONSOLE_PROMPT);
            System.out.println();

            logInfo("Debug console exited");
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

        try
        {
            reconfigurationLock.writeLock().lock();
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
                stopSystemServices(systemServicesMap.values(), getErrorReporter());

                if (workerThrPool != null)
                {
                    logInfo("Shutting down worker thread pool");
                    workerThrPool.shutdown();
                    workerThrPool = null;
                }

                logInfo("Shutdown complete");
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
        logWarning("Main network communications service initialization is not implemented yet.");
    }

    @Override
    public final void logInit(String message)
    {
        // TODO: Log at the INFO level
        System.out.println("INIT      " + message);
    }

    @Override
    public final void logInfo(String message)
    {
        // TODO: Log at the INFO level
        System.out.println("INFO      " + message);
    }

    @Override
    public final void logWarning(String message)
    {
        // TODO: Log at the WARNING level
        System.out.println("WARNING   " + message);
    }

    @Override
    public final void logError(String message)
    {
        // TODO: Log at the ERROR level
        System.out.println("ERROR     " + message);
    }

    @Override
    public final void logFailure(String message)
    {
        // TODO: Log at the ERROR level
        System.err.println("FAILED    " + message);
    }

    @Override
    public final void logDebug(String message)
    {
        // TODO: Log at the DEBUG level
        System.err.println("DEBUG     " + message);
    }

    public static void main(String[] args)
    {
        System.out.printf(
            "%s, Module %s, Release %s\n",
            Satellite.PROGRAM, Satellite.MODULE, Satellite.VERSION
        );
        printStartupInfo();

        ErrorReporter errorLog = new DebugErrorReporter(System.err);

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

    private class ConnTracker implements ConnectionObserver
    {
        private Satellite satellite;

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
                peerMap.put(connPeer.getId(), connPeer);
            }
        }

        @Override
        public void inboundConnectionEstablished(Peer connPeer)
        {
            if (connPeer != null)
            {
                SatellitePeerCtx peerCtx = new SatellitePeerCtx();
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
    }

    private static class DebugControlImpl implements DebugControl
    {
        Satellite satellite;

        DebugControlImpl(Satellite satelliteRef)
        {
            satellite = satelliteRef;
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
