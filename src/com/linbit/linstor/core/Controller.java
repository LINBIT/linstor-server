package com.linbit.linstor.core;

import com.google.inject.Guice;
import javax.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.linbit.ControllerLinstorModule;
import com.linbit.GuiceConfigModule;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.drbd.md.MetaDataModule;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.Node;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiType;
import com.linbit.linstor.core.apicallhandler.ApiCallHandlerModule;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandlerModule;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbcp.DbConnectionPoolModule;
import com.linbit.linstor.dbdrivers.ControllerDbModule;
import com.linbit.linstor.debug.ControllerDebugModule;
import com.linbit.linstor.debug.DebugConsole;
import com.linbit.linstor.debug.DebugConsoleCreator;
import com.linbit.linstor.debug.DebugConsoleImpl;
import com.linbit.linstor.debug.DebugModule;
import com.linbit.linstor.event.ControllerEventModule;
import com.linbit.linstor.event.EventModule;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.event.writer.protobuf.ProtobufEventWriter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.netcom.NetComModule;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.DbAccessor;
import com.linbit.linstor.security.Initializer;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SecurityLevel;
import com.linbit.linstor.security.SecurityModule;
import com.linbit.linstor.tasks.ErrorReportTimeOutTask;
import com.linbit.linstor.tasks.GarbageCollectorTask;
import com.linbit.linstor.tasks.PingTask;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.linstor.tasks.TaskScheduleService;
import com.linbit.linstor.timer.CoreTimer;
import com.linbit.linstor.timer.CoreTimerModule;
import com.linbit.linstor.transaction.ControllerTransactionMgrModule;

import javax.inject.Named;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.regex.Pattern;

/**
 * linstor controller prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Controller
{
    // System module information
    public static final String MODULE = "Controller";

    private static final String PROPSCON_KEY_NETCOM = "netcom";

    public static final int API_VERSION = 1;
    public static final int API_MIN_VERSION = API_VERSION;

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

    private final ApplicationLifecycleManager applicationLifecycleManager;

    // Controller configuration properties
    private final Props ctrlConf;

    // Map of all managed nodes
    private final CoreModule.NodesMap nodesMap;

    private final TaskScheduleService taskScheduleService;
    private final PingTask pingTask;
    private final ReconnectorTask reconnectorTask;
    private final ErrorReportTimeOutTask errorReportTimeOutTask;

    private final DebugConsoleCreator debugConsoleCreator;
    private final SatelliteConnector satelliteConnector;
    private final ControllerNetComInitializer controllerNetComInitializer;

    @Inject
    public Controller(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext sysCtxRef,
        CoreTimer timerEventSvcRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        Map<ServiceName, SystemService> systemServicesMapRef,
        DbConnectionPool dbConnPoolRef,
        ApplicationLifecycleManager applicationLifecycleManagerRef,
        @Named(ControllerCoreModule.CONTROLLER_PROPS) Props ctrlConfRef,
        CoreModule.NodesMap nodesMapRef,
        TaskScheduleService taskScheduleServiceRef, PingTask pingTaskRef,
        ReconnectorTask reconnectorTaskRef,
        ErrorReportTimeOutTask errorReportTimeOutTaskRef,
        DebugConsoleCreator debugConsoleCreatorRef,
        SatelliteConnector satelliteConnectorRef,
        ControllerNetComInitializer controllerNetComInitializerRef
    )
    {
        errorReporter = errorReporterRef;
        sysCtx = sysCtxRef;
        timerEventSvc = timerEventSvcRef;
        reconfigurationLock = reconfigurationLockRef;
        systemServicesMap = systemServicesMapRef;
        dbConnPool = dbConnPoolRef;
        applicationLifecycleManager = applicationLifecycleManagerRef;
        ctrlConf = ctrlConfRef;
        nodesMap = nodesMapRef;
        taskScheduleService = taskScheduleServiceRef;
        pingTask = pingTaskRef;
        reconnectorTask = reconnectorTaskRef;
        errorReportTimeOutTask = errorReportTimeOutTaskRef;
        debugConsoleCreator = debugConsoleCreatorRef;
        satelliteConnector = satelliteConnectorRef;
        controllerNetComInitializer = controllerNetComInitializerRef;
    }

    public void start()
        throws InvalidKeyException
    {
        applicationLifecycleManager.installShutdownHook();

        reconfigurationLock.writeLock().lock();

        try
        {
            AccessContext initCtx = sysCtx.clone();
            initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            systemServicesMap.put(dbConnPool.getInstanceName(), dbConnPool);
            systemServicesMap.put(taskScheduleService.getInstanceName(), taskScheduleService);
            systemServicesMap.put(timerEventSvc.getInstanceName(), timerEventSvc);

            taskScheduleService.addTask(pingTask);
            taskScheduleService.addTask(reconnectorTask);
            taskScheduleService.addTask(errorReportTimeOutTask);
            taskScheduleService.addTask(new GarbageCollectorTask());

            controllerNetComInitializer.initNetComServices(
                ctrlConf.getNamespace(PROPSCON_KEY_NETCOM).orElse(null),
                errorReporter,
                initCtx
            );

            applicationLifecycleManager.startSystemServices(systemServicesMap.values());

            connectToKnownNodes(errorReporter, initCtx);

            errorReporter.logInfo("Controller initialized");
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
        System.setProperty("log.module", MODULE);

        LinStorArguments cArgs = LinStorArgumentParser.parseCommandLine(args);

        System.out.printf(
            "%s, Module %s\n",
            LinStor.PROGRAM, Controller.MODULE
        );
        LinStor.printStartupInfo();

        ErrorReporter errorLog = new StdErrorReporter(
            Controller.MODULE,
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
            List<String> packageSuffixes = Arrays.asList("common", "controller");

            List<Class<? extends ApiCall>> apiCalls = classPathLoader.loadClasses(
                ProtobufApiType.class.getPackage().getName(),
                packageSuffixes,
                ApiCall.class,
                ProtobufApiCall.class
            );

            List<Class<? extends EventWriter>> eventWriters = classPathLoader.loadClasses(
                ProtobufEventWriter.class.getPackage().getName(),
                packageSuffixes,
                EventWriter.class,
                ProtobufEventWriter.class
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
                new LinStorArgumentsModule(cArgs),
                new ConfigModule(),
                new CoreTimerModule(),
                new MetaDataModule(),
                new ControllerLinstorModule(),
                new LinStorModule(),
                new CoreModule(),
                new ControllerCoreModule(),
                new ControllerSatelliteConnectorModule(),
                new ControllerDbModule(),
                new DbConnectionPoolModule(),
                new NetComModule(),
                new NumberPoolModule(),
                new ApiModule(apiType, apiCalls),
                new ApiCallHandlerModule(),
                new CtrlApiCallHandlerModule(),
                new EventModule(eventWriters, eventHandlers),
                new ControllerEventModule(),
                new DebugModule(),
                new ControllerDebugModule(),
                new ControllerTransactionMgrModule()
            );
            errorLog.logInfo(String.format(
                    "Dependency injection finished: %dms",
                    (System.currentTimeMillis() - startDepInjectionTime))
            );

            // Object protection loading has a hidden dependency on initializing the security objects
            // (via com.linbit.linstor.security.Role.GLOBAL_ROLE_MAP).
            // Hence the security objects should be initialized first.
            initializeSecurityObjects(
                errorLog,
                injector.getInstance(Key.get(AccessContext.class, SystemContext.class)),
                injector.getInstance(ControllerDatabase.class),
                injector.getInstance(DbAccessor.class)
            );

            Controller instance = injector.getInstance(Controller.class);
            instance.start();

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

    private static void initializeSecurityObjects(
        ErrorReporter errorLogRef,
        AccessContext initCtx,
        ControllerDatabase ctrlDb,
        DbAccessor driver
    )
        throws InitializationException
    {
        // Load security identities, roles, domains/types, etc.
        errorLogRef.logInfo("Loading security objects");
        try
        {
            Initializer.load(initCtx, ctrlDb, driver);
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
}
