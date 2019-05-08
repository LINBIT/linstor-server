package com.linbit.linstor.core;

import com.linbit.GuiceConfigModule;
import com.linbit.ImplementationError;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.drbd.md.MetaDataModule;
import com.linbit.linstor.ControllerLinstorModule;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.Node;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.BaseApiCall;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiType;
import com.linbit.linstor.api.rest.v1.GrizzlyHttpService;
import com.linbit.linstor.core.apicallhandler.ApiCallHandlerModule;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandlerModule;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbcp.DbConnectionPoolInitializer;
import com.linbit.linstor.dbcp.DbConnectionPoolModule;
import com.linbit.linstor.dbdrivers.ControllerDbModule;
import com.linbit.linstor.debug.ControllerDebugModule;
import com.linbit.linstor.debug.DebugConsole;
import com.linbit.linstor.debug.DebugConsoleCreator;
import com.linbit.linstor.debug.DebugConsoleImpl;
import com.linbit.linstor.debug.DebugModule;
import com.linbit.linstor.event.EventModule;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.ProtobufEventSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.netcom.NetComModule;
import com.linbit.linstor.numberpool.DbNumberPoolInitializer;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.DbCoreObjProtInitializer;
import com.linbit.linstor.security.DbSecurityInitializer;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SecurityModule;
import com.linbit.linstor.tasks.PingTask;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.linstor.tasks.RetryResourcesTask;
import com.linbit.linstor.tasks.TaskScheduleService;
import com.linbit.linstor.timer.CoreTimer;
import com.linbit.linstor.timer.CoreTimerModule;
import com.linbit.linstor.transaction.ControllerTransactionMgrModule;

import javax.inject.Inject;
import javax.inject.Named;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * linstor controller prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Controller
{
    private static final String PROPSCON_KEY_NETCOM = "netcom";

    public static final int API_VERSION = 4;
    public static final int API_MIN_VERSION = API_VERSION;

    private static final String ENV_REST_BIND_ADDRESS = "LS_REST_BIND_ADDRESS";
    private static final String DEFAULT_HTTP_LISTEN_ADDRESS = "[::]";
    private static final String DEFAULT_HTTP_REST_PORT = "3370";

    // Error & exception logging facility
    private final ErrorReporter errorReporter;

    // System security context
    private final AccessContext sysCtx;

    private final CoreTimer timerEventSvc;

    // Synchronization lock for major global changes
    private final ReadWriteLock reconfigurationLock;

    // Map of controllable system services
    private final Map<ServiceName, SystemService> systemServicesMap;

    // Database connection pool service
    private final DbConnectionPool dbConnPool;

    private final DbConnectionPoolInitializer dbConnectionPoolInitializer;
    private final DbSecurityInitializer dbSecurityInitializer;
    private final DbCoreObjProtInitializer dbCoreObjProtInitializer;
    private final DbDataInitializer dbDataInitializer;
    private final DbNumberPoolInitializer dbNumberPoolInitializer;

    private final ApplicationLifecycleManager applicationLifecycleManager;

    // Controller configuration properties
    private final Props ctrlConf;

    // Map of all managed nodes
    private final CoreModule.NodesMap nodesMap;

    private final TaskScheduleService taskScheduleService;
    private final PingTask pingTask;
    private final ReconnectorTask reconnectorTask;

    private final DebugConsoleCreator debugConsoleCreator;
    private final SatelliteConnector satelliteConnector;
    private final ControllerNetComInitializer controllerNetComInitializer;

    private final SwordfishTargetProcessManager swordfishTargetProcessManager;
    private final WhitelistProps whitelistProps;

    private RetryResourcesTask retryResourcesTask;

    @Inject
    public Controller(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext sysCtxRef,
        CoreTimer timerEventSvcRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        Map<ServiceName, SystemService> systemServicesMapRef,
        DbConnectionPool dbConnPoolRef,
        DbConnectionPoolInitializer dbConnectionPoolInitializerRef,
        DbSecurityInitializer dbSecurityInitializerRef,
        DbCoreObjProtInitializer dbCoreObjProtInitializerRef,
        DbDataInitializer dbDataInitializerRef,
        DbNumberPoolInitializer dbNumberPoolInitializerRef,
        ApplicationLifecycleManager applicationLifecycleManagerRef,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef,
        CoreModule.NodesMap nodesMapRef,
        TaskScheduleService taskScheduleServiceRef,
        PingTask pingTaskRef,
        ReconnectorTask reconnectorTaskRef,
        RetryResourcesTask retryResourcesTaskRef,
        DebugConsoleCreator debugConsoleCreatorRef,
        SatelliteConnector satelliteConnectorRef,
        ControllerNetComInitializer controllerNetComInitializerRef,
        SwordfishTargetProcessManager swordfishTargetProcessManagerRef,
        WhitelistProps whitelistPropsRef
    )
    {
        errorReporter = errorReporterRef;
        sysCtx = sysCtxRef;
        timerEventSvc = timerEventSvcRef;
        reconfigurationLock = reconfigurationLockRef;
        systemServicesMap = systemServicesMapRef;
        dbConnPool = dbConnPoolRef;
        dbConnectionPoolInitializer = dbConnectionPoolInitializerRef;
        dbSecurityInitializer = dbSecurityInitializerRef;
        dbCoreObjProtInitializer = dbCoreObjProtInitializerRef;
        dbDataInitializer = dbDataInitializerRef;
        dbNumberPoolInitializer = dbNumberPoolInitializerRef;
        applicationLifecycleManager = applicationLifecycleManagerRef;
        ctrlConf = ctrlConfRef;
        nodesMap = nodesMapRef;
        taskScheduleService = taskScheduleServiceRef;
        pingTask = pingTaskRef;
        reconnectorTask = reconnectorTaskRef;
        retryResourcesTask = retryResourcesTaskRef;
        debugConsoleCreator = debugConsoleCreatorRef;
        satelliteConnector = satelliteConnectorRef;
        controllerNetComInitializer = controllerNetComInitializerRef;
        swordfishTargetProcessManager = swordfishTargetProcessManagerRef;
        whitelistProps = whitelistPropsRef;
    }

    public void start(Injector injector, ControllerCmdlArguments cArgs)
        throws SystemServiceStartException, InitializationException
    {
        applicationLifecycleManager.installShutdownHook();

        reconfigurationLock.writeLock().lock();

        try
        {
            AccessContext initCtx = sysCtx.clone();
            initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            taskScheduleService.addTask(pingTask);
            taskScheduleService.addTask(reconnectorTask);
            taskScheduleService.addTask(retryResourcesTask);

            systemServicesMap.put(dbConnPool.getInstanceName(), dbConnPool);
            systemServicesMap.put(taskScheduleService.getInstanceName(), taskScheduleService);
            systemServicesMap.put(timerEventSvc.getInstanceName(), timerEventSvc);

            dbConnectionPoolInitializer.initialize();

            // Object protection loading has a hidden dependency on initializing the security objects
            // (via com.linbit.linstor.security.Role.GLOBAL_ROLE_MAP).
            // Hence the security objects should be initialized first.
            dbSecurityInitializer.initialize();

            dbCoreObjProtInitializer.initialize();
            dbDataInitializer.initialize();
            dbNumberPoolInitializer.initialize();

            initializeRestServer(injector, cArgs);

            applicationLifecycleManager.startSystemServices(systemServicesMap.values());

            controllerNetComInitializer.initNetComServices(
                ctrlConf.getNamespace(PROPSCON_KEY_NETCOM).orElse(null),
                errorReporter,
                initCtx
            );

            whitelistProps.overrideDrbdProperties();

            swordfishTargetProcessManager.initialize();

            connectToKnownNodes(errorReporter, initCtx);

            errorReporter.logInfo("Controller initialized");
        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ImplementationError(
              "Invalid key used.",
              invalidKeyExc
            );
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

    private void initializeRestServer(Injector injector, ControllerCmdlArguments cArgs) throws InvalidKeyException
    {
        boolean restEnabled = true;
        String restBindAddress; // = String.format("%s:%d", DEFAULT_HTTP_LISTEN_ADDRESS, DEFAULT_HTTP_REST_PORT);

        if (cArgs.getRESTBindAddress() != null)
        {
            restBindAddress = cArgs.getRESTBindAddress();
        }
        else
        {
            final String envRESTBindAddress = System.getenv(ENV_REST_BIND_ADDRESS);
            if (envRESTBindAddress != null)
            {
                restBindAddress = envRESTBindAddress;
            }
            else
            {
                restEnabled = Boolean.parseBoolean(
                    ctrlConf.getPropWithDefault(ApiConsts.KEY_ENABLED, ApiConsts.NAMESPC_REST, "true")
                );
                String restListenAddr = ctrlConf.getPropWithDefault(
                    ApiConsts.KEY_BIND_ADDR, ApiConsts.NAMESPC_REST, DEFAULT_HTTP_LISTEN_ADDRESS
                );
                int restListenPort = Integer.parseInt(
                    ctrlConf.getPropWithDefault(ApiConsts.KEY_BIND_PORT, ApiConsts.NAMESPC_REST, DEFAULT_HTTP_REST_PORT)
                );
                restBindAddress = String.format("%s:%d", restListenAddr, restListenPort);
            }
        }

        if (restEnabled)
        {
            final GrizzlyHttpService grizzlyHttpService = new GrizzlyHttpService(
                injector, errorReporter.getLogDirectory(), restBindAddress
            );
            systemServicesMap.put(grizzlyHttpService.getInstanceName(), grizzlyHttpService);
        }
    }

    private void enterDebugConsole()
    {
        try
        {
            errorReporter.logInfo("Entering debug console");

            AccessContext privCtx = sysCtx.clone();
            AccessContext debugCtx = sysCtx.clone();
            privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
            debugCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            DebugConsole dbgConsole = debugConsoleCreator.createDebugConsole(privCtx, debugCtx, null);
            dbgConsole.stdStreamsConsole(DebugConsoleImpl.CONSOLE_PROMPT);
            System.out.println();

            errorReporter.logInfo("Debug console exited");
        }
        catch (Throwable error)
        {
            errorReporter.reportError(error);
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

    private void connectToKnownNodes(final ErrorReporter errorLogRef, final AccessContext initCtx)
    {
        if (!nodesMap.isEmpty())
        {
            errorLogRef.logInfo("Reconnecting to previously known nodes");
            Collection<Node> nodes = nodesMap.values();
            for (Node node : nodes)
            {
                errorLogRef.logDebug("Reconnecting to node '" + node.getName() + "'.");
                satelliteConnector.startConnecting(node, initCtx);
            }
            errorLogRef.logInfo("Reconnect requests sent");
        }
        else
        {
            errorLogRef.logInfo("No known nodes.");
        }
    }

    public static void main(String[] args)
    {
        ControllerCmdlArguments cArgs = ControllerArgumentParser.parseCommandLine(args);

        System.setProperty("log.module", LinStor.CONTROLLER_MODULE);
        System.setProperty("log.directory", cArgs.getLogDirectory());

        System.out.printf(
            "%s, Module %s\n",
            LinStor.PROGRAM, LinStor.CONTROLLER_MODULE
        );
        LinStor.printStartupInfo();

        ErrorReporter errorLog = new StdErrorReporter(
            LinStor.CONTROLLER_MODULE,
            Paths.get(cArgs.getLogDirectory()),
            cArgs.isPrintStacktraces(),
            LinStor.getHostName()
        );

        try
        {
            Thread.currentThread().setName("Main");

            errorLog.logInfo("Loading API classes started.");
            long startAPIClassLoadingTime = System.currentTimeMillis();
            ApiType apiType = new ProtobufApiType();
            ClassPathLoader classPathLoader = new ClassPathLoader(errorLog);
            List<String> packageSuffixes = Arrays.asList("common", "controller", "internal");

            List<Class<? extends BaseApiCall>> apiCalls = classPathLoader.loadClasses(
                ProtobufApiType.class.getPackage().getName(),
                packageSuffixes,
                BaseApiCall.class,
                ProtobufApiCall.class
            );

            List<Class<? extends EventSerializer>> eventSerializers = classPathLoader.loadClasses(
                ProtobufEventSerializer.class.getPackage().getName(),
                packageSuffixes,
                EventSerializer.class,
                ProtobufEventSerializer.class
            );

            List<Class<? extends EventHandler>> eventHandlers = classPathLoader.loadClasses(
                ProtobufEventHandler.class.getPackage().getName(),
                packageSuffixes,
                EventHandler.class,
                ProtobufEventHandler.class
            );
            errorLog.logInfo(String.format(
                "API classes loading finished: %dms",
                (System.currentTimeMillis() - startAPIClassLoadingTime))
            );

            errorLog.logInfo("Dependency injection started.");
            long startDepInjectionTime = System.currentTimeMillis();
            Injector injector = Guice.createInjector(
                new GuiceConfigModule(),
                new LoggingModule(errorLog),
                new SecurityModule(),
                new ControllerSecurityModule(),
                new ControllerArgumentsModule(cArgs),
                new CoreTimerModule(),
                new MetaDataModule(),
                new ControllerLinstorModule(),
                new LinStorModule(),
                new CoreModule(),
                new ControllerCoreModule(),
                new ControllerSatelliteCommunicationModule(),
                new ControllerDbModule(),
                new DbConnectionPoolModule(),
                new NetComModule(),
                new NumberPoolModule(),
                new ApiModule(apiType, apiCalls),
                new ApiCallHandlerModule(),
                new CtrlApiCallHandlerModule(),
                new EventModule(eventSerializers, eventHandlers),
                new DebugModule(),
                new ControllerDebugModule(),
                new ControllerTransactionMgrModule()
            );
            errorLog.logInfo(String.format(
                    "Dependency injection finished: %dms",
                    (System.currentTimeMillis() - startDepInjectionTime))
            );

            Controller instance = injector.getInstance(Controller.class);
            instance.start(injector, cArgs);

            if (cArgs.startDebugConsole())
            {
                instance.enterDebugConsole();
            }
        }
        catch (Throwable error)
        {
            errorLog.reportError(error);
            System.exit(InternalApiConsts.EXIT_CODE_IMPL_ERROR);
        }

        System.out.println();
    }
}
