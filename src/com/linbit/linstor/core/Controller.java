package com.linbit.linstor.core;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.SystemServiceStopException;
import com.linbit.TransactionMgr;
import com.linbit.WorkerPool;
import com.linbit.linstor.ControllerPeerCtx;
import com.linbit.linstor.CoreServices;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbdrivers.DatabaseDriver;
import com.linbit.linstor.debug.DebugConsole;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.netcom.NetComContainer;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.netcom.TcpConnectorService;
import com.linbit.linstor.netcom.ssl.SslTcpConnectorService;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.CommonMessageProcessor;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.Authentication;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.DbAccessor;
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
import org.slf4j.event.Level;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.regex.Pattern;

/**
 * linstor controller prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Controller extends LinStor implements CoreServices
{
    // System module information
    public static final String MODULE = "Controller";

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

    public static final int API_VERSION = 1;
    public static final int API_MIN_VERSION = API_VERSION;

    public static final Pattern RANGE_PATTERN = Pattern.compile("(?<min>\\d+) ?- ?(?<max>\\d+)");

    private final Injector injector;

    // System security context
    private AccessContext sysCtx;

    // Public security context
    private AccessContext publicCtx;

    private CtrlApiCallHandler apiCallHandler;

    // ============================================================
    // Worker thread pool & message processing dispatcher
    //
    private WorkerPool workerThrPool = null;
    private CommonMessageProcessor msgProc;

    // Authentication & Authorization subsystems
    private Authentication idAuthentication = null;

    // ============================================================
    // Core system services
    //
    // Map of controllable system services
    private Map<ServiceName, SystemService> systemServicesMap;

    // Database connection pool service
    DbConnectionPool dbConnPool;

    // Satellite reconnector service
    private TaskScheduleService taskScheduleService;

    // Map of connected peers
    private Map<String, Peer> peerMap;

    private NetComContainer netComContainer;

    private ApplicationLifecycleManager applicationLifecycleManager;

    // Synchronization lock for the configuration
    public ReadWriteLock ctrlConfLock;

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

    private ReconnectorTask reconnectorTask;

    public Controller(
        Injector injectorRef,
        AccessContext sysCtxRef,
        AccessContext publicCtxRef
    )
    {
        injector = injectorRef;

        // Initialize security contexts
        sysCtx = sysCtxRef;
        publicCtx = publicCtxRef;
    }

    public static void main(String[] args)
    {
        LinStorArguments cArgs = LinStorArgumentParser.parseCommandLine(args);

        System.out.printf(
            "%s, Module %s\n",
            Controller.PROGRAM, Controller.MODULE
        );
        printStartupInfo();

        ErrorReporter errorLog = new StdErrorReporter(Controller.MODULE, cArgs.getWorkingDirectory());

        try
        {
            Thread.currentThread().setName("Main");

            // Initialize the Controller module with the SYSTEM security context
            Initializer sysInit = new Initializer();
            Controller instance = sysInit.initController(cArgs, errorLog);

            instance.initialize();
            if (cArgs.startDebugConsole())
            {
                instance.enterDebugConsole();
            }
        }
        catch (Throwable error)
        {
            errorLog.reportError(error);
        }

        System.out.println();
    }

    public void initialize()
        throws InitializationException, InvalidKeyException
    {
        reconfigurationLock = injector.getInstance(
            Key.get(ReadWriteLock.class, Names.named(CoreModule.RECONFIGURATION_LOCK)));
        nodesMapLock = injector.getInstance(
            Key.get(ReadWriteLock.class, Names.named(CoreModule.NODES_MAP_LOCK)));
        rscDfnMapLock = injector.getInstance(
            Key.get(ReadWriteLock.class, Names.named(CoreModule.RSC_DFN_MAP_LOCK)));
        storPoolDfnMapLock = injector.getInstance(
            Key.get(ReadWriteLock.class, Names.named(CoreModule.STOR_POOL_DFN_MAP_LOCK)));
        ctrlConfLock = injector.getInstance(
            Key.get(ReadWriteLock.class, Names.named(ControllerCoreModule.CTRL_CONF_LOCK)));

        reconfigurationLock.writeLock().lock();

        try
        {
            AccessContext initCtx = sysCtx.clone();
            initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            ErrorReporter errorLogRef = injector.getInstance(ErrorReporter.class);

            // Initialize the error & exception reporting facility
            setErrorLog(initCtx, errorLogRef);

            systemServicesMap = injector.getInstance(Key.get(new TypeLiteral<Map<ServiceName, SystemService>>() {}));

            taskScheduleService = new TaskScheduleService(this);
            systemServicesMap.put(taskScheduleService.getInstanceName(), taskScheduleService);

            timerEventSvc = injector.getInstance(CoreTimer.class);
            {
                CoreTimer timer = super.getTimer();
                systemServicesMap.put(timer.getInstanceName(), timer);
            }

            securityDbDriver = injector.getInstance(DbAccessor.class);
            persistenceDbDriver = injector.getInstance(DatabaseDriver.class);

            dbConnPool = injector.getInstance(DbConnectionPool.class);
            systemServicesMap.put(dbConnPool.getInstanceName(), dbConnPool);

            // Initialize LinStor objects maps
            peerMap = injector.getInstance(CoreModule.PeerMap.class);
            nodesMap = injector.getInstance(CoreModule.NodesMap.class);
            rscDfnMap = injector.getInstance(CoreModule.ResourceDefinitionMap.class);
            storPoolDfnMap = injector.getInstance(CoreModule.StorPoolDefinitionMap.class);

            idAuthentication = injector.getInstance(Authentication.class);

            ctrlConf = injector.getInstance(Key.get(Props.class, Names.named(ControllerCoreModule.CONTROLLER_PROPS)));

            // Object protection loading has a hidden dependency on initializing the security objects
            // (via com.linbit.linstor.security.Role.GLOBAL_ROLE_MAP)
            initializeSecurityObjects(errorLogRef, initCtx);

            applicationLifecycleManager = injector.getInstance(ApplicationLifecycleManager.class);

            nodesMapProt = injector.getInstance(
                Key.get(ObjectProtection.class, Names.named(ControllerSecurityModule.NODES_MAP_PROT)));
            rscDfnMapProt = injector.getInstance(
                Key.get(ObjectProtection.class, Names.named(ControllerSecurityModule.RSC_DFN_MAP_PROT)));
            storPoolDfnMapProt = injector.getInstance(
                Key.get(ObjectProtection.class, Names.named(ControllerSecurityModule.STOR_POOL_DFN_MAP_PROT)));
            ctrlConfProt = injector.getInstance(
                Key.get(ObjectProtection.class, Names.named(ControllerSecurityModule.CTRL_CONF_PROT)));

            disklessStorPoolDfn = injector.getInstance(
                Key.get(StorPoolDefinitionData.class, Names.named(LinStorModule.DISKLESS_STOR_POOL_DFN)));

            errorLogRef.logInfo("Core objects load from database is in progress");
            loadCoreObjects(initCtx);
            errorLogRef.logInfo("Core objects load from database completed");

            netComContainer = injector.getInstance(NetComContainer.class);

            workerThrPool = injector.getInstance(WorkerPool.class);

            apiCallHandler = injector.getInstance(CtrlApiCallHandler.class);

            // Initialize tasks
            reconnectorTask = injector.getInstance(ReconnectorTask.class);
            PingTask pingTask = injector.getInstance(PingTask.class);
            taskScheduleService.addTask(pingTask);
            taskScheduleService.addTask(reconnectorTask);

            taskScheduleService.addTask(new GarbageCollectorTask());

            // Initialize the message processor
            msgProc = injector.getInstance(CommonMessageProcessor.class);

            errorLogRef.logInfo("Initializing test APIs");
            LinStor.loadApiCalls(msgProc, this, this, ApiType.PROTOBUF);

            initNetComServices(
                ctrlConf.getNamespace(PROPSCON_KEY_NETCOM),
                errorLogRef,
                initCtx
            );

            applicationLifecycleManager.startSystemServices(systemServicesMap.values());

            connectToKnownNodes(errorLogRef, initCtx);

            errorLogRef.logInfo("Controller initialized");
        }
        catch (AccessDeniedException accessExc)
        {
            throw new ImplementationError(
                "The initialization security context does not have all privileges. " +
                "Initialization failed.",
                accessExc
            );
        }
        finally
        {
            reconfigurationLock.writeLock().unlock();
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

    public void enterDebugConsole()
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
            applicationLifecycleManager.shutdown(shutdownCtx);
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

    public void requireShutdownAccess(AccessContext accCtx) throws AccessDeniedException
    {
        applicationLifecycleManager.requireShutdownAccess(accCtx);
    }

    public void shutdown(AccessContext accCtx) throws AccessDeniedException
    {
        applicationLifecycleManager.shutdown(accCtx);
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
        TcpConnector netComSvc = netComContainer.getNetComConnector(serviceName);
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

        netComContainer.removeNetComConnector(serviceName);
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

    public CtrlApiCallHandler getApiCallHandler()
    {
        return apiCallHandler;
    }

    private void initializeSecurityObjects(final ErrorReporter errorLogRef, final AccessContext initCtx)
        throws InitializationException
    {
        // Load security identities, roles, domains/types, etc.
        errorLogRef.logInfo("Loading security objects");
        try
        {
            Initializer.load(initCtx, dbConnPool, securityDbDriver);
        }
        catch (SQLException | InvalidNameException | AccessDeniedException exc)
        {
            throw new InitializationException("Failed to load security objects", exc);
        }

        errorLogRef.logInfo(
            "Current security level is %s",
            SecurityLevel.get().name()
        );
    }

    private void loadCoreObjects(AccessContext initCtx)
        throws AccessDeniedException, InitializationException
    {
        TransactionMgr transMgr = null;
        try
        {
            transMgr = new TransactionMgr(dbConnPool);
            nodesMapProt.requireAccess(initCtx, AccessType.CONTROL);
            rscDfnMapProt.requireAccess(initCtx, AccessType.CONTROL);
            storPoolDfnMapProt.requireAccess(initCtx, AccessType.CONTROL);

            Lock recfgWriteLock = reconfigurationLock.writeLock();
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
        catch (SQLException exc)
        {
            throw new InitializationException(
                "Loading the core objects from the database failed",
                exc
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
                dbConnPool.returnConnection(transMgr);
            }
        }
    }

    private void initNetComServices(
        Props netComProps,
        ErrorReporter errorLogRef,
        AccessContext initCtx
    )
    {
        errorLogRef.logInfo("Initializing network communications services");

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
                    createNetComService(
                        namespaceStr,
                        netComProps,
                        errorLogRef,
                        initCtx
                    );
                }
                catch (SystemServiceStartException sysSvcStartExc)
                {
                    errorLogRef.reportProblem(Level.ERROR, sysSvcStartExc, null, null, null);
                }
            }
        }
    }

    private void createNetComService(
        String serviceNameStr,
        Props netComProps,
        ErrorReporter errorLogRef,
        AccessContext initCtx
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
                initCtx,
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
                    bindAddress,
                    publicCtx,
                    initCtx,
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
            netComContainer.putNetComContainer(serviceName, netComSvc);
            systemServicesMap.put(serviceName, netComSvc);
            errorLogRef.logInfo(
                String.format(
                    "Created network communication service '%s', bound to %s:%d",
                    serviceName.displayValue, bindAddressStr, port
                )
            );
        }
    }

    private String loadPropChecked(Props props, String key) throws SystemServiceStartException
    {
        String value;
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

    private void connectToKnownNodes(final ErrorReporter errorLogRef, final AccessContext initCtx)
    {
        if (!nodesMap.isEmpty())
        {
            errorLogRef.logInfo("Reconnecting to previously known nodes");
            Collection<Node> nodes = nodesMap.values();
            for (Node node : nodes)
            {
                errorLogRef.logDebug("Reconnecting to node '" + node.getName() + "'.");
                CtrlNodeApiCallHandler ctrlNodeApiCallHandler = injector.getInstance(CtrlNodeApiCallHandler.class);
                ctrlNodeApiCallHandler.startConnecting(node, initCtx);
            }
            errorLogRef.logInfo("Reconnect requests sent");
        }
        else
        {
            errorLogRef.logInfo("No known nodes.");
        }
    }
}
