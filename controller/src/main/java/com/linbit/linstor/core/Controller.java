package com.linbit.linstor.core;

import com.linbit.GuiceConfigModule;
import com.linbit.ImplementationError;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.drbd.md.MetaDataModule;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerLinstorModule;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.BaseApiCall;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiType;
import com.linbit.linstor.core.apicallhandler.ApiCallHandlerModule;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandlerModule;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.core.cfg.CtrlConfigModule;
import com.linbit.linstor.core.ebs.EbsStatusManagerService;
import com.linbit.linstor.dbcp.DbInitializer;
import com.linbit.linstor.dbcp.migration.AbsMigration;
import com.linbit.linstor.dbdrivers.ControllerDbModule;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.debug.ControllerDebugModule;
import com.linbit.linstor.debug.DebugConsole;
import com.linbit.linstor.debug.DebugConsoleCreator;
import com.linbit.linstor.debug.DebugConsoleImpl;
import com.linbit.linstor.debug.DebugModule;
import com.linbit.linstor.event.EventModule;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.protobuf.controller.ConnectionStateEventHandler;
import com.linbit.linstor.event.handler.protobuf.controller.DonePercentageEventHandler;
import com.linbit.linstor.event.handler.protobuf.controller.ReplicationStateEventHandler;
import com.linbit.linstor.event.handler.protobuf.controller.ResourceStateEventHandler;
import com.linbit.linstor.event.handler.protobuf.controller.VolumeDiskStateEventHandler;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.common.ConnectionStateEventSerializer;
import com.linbit.linstor.event.serializer.protobuf.common.DonePercentageEventSerializer;
import com.linbit.linstor.event.serializer.protobuf.common.ReplicationStateEventSerializer;
import com.linbit.linstor.event.serializer.protobuf.common.ResourceStateEventSerializer;
import com.linbit.linstor.event.serializer.protobuf.common.VolumeDiskStateEventSerializer;
import com.linbit.linstor.layer.LayerSizeCalculatorModule;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;
import com.linbit.linstor.netcom.NetComModule;
import com.linbit.linstor.numberpool.DbNumberPoolInitializer;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.DbCoreObjProtInitializer;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SecurityModule;
import com.linbit.linstor.systemstarter.ConnectNodesInitializer;
import com.linbit.linstor.systemstarter.GrizzlyInitializer;
import com.linbit.linstor.systemstarter.PassphraseInitializer;
import com.linbit.linstor.systemstarter.PreConnectInitializer;
import com.linbit.linstor.systemstarter.ResyncAfterInitializer;
import com.linbit.linstor.systemstarter.ServiceStarter;
import com.linbit.linstor.systemstarter.SpecStltProcMgrInit;
import com.linbit.linstor.systemstarter.StartupInitializer;
import com.linbit.linstor.tasks.AutoDiskfulTask;
import com.linbit.linstor.tasks.AutoSnapshotTask;
import com.linbit.linstor.tasks.BalanceResourcesTask;
import com.linbit.linstor.tasks.LogArchiveTask;
import com.linbit.linstor.tasks.PingTask;
import com.linbit.linstor.tasks.QsiClearCacheTask;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.linstor.tasks.RetryResourcesTask;
import com.linbit.linstor.tasks.ScheduleBackupService;
import com.linbit.linstor.tasks.TaskScheduleService;
import com.linbit.linstor.tasks.UpdateSpaceInfoTask;
import com.linbit.linstor.timer.CoreTimer;
import com.linbit.linstor.timer.CoreTimerModule;
import com.linbit.linstor.transaction.ControllerTransactionMgrModule;
import com.linbit.linstor.utils.NameShortenerModule;
import com.linbit.utils.InjectorLoader;

import javax.inject.Inject;
import javax.inject.Named;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.ProvisionException;
import com.google.inject.name.Names;
import org.slf4j.MDC;
import org.slf4j.event.Level;

/**
 * linstor controller prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Controller
{
    private static final String PROPSCON_KEY_NETCOM = "netcom";
    public static final String SPC_TRK_MODULE_NAME = "com.linbit.linstor.spacetracking.ControllerSpaceTrackingModule";
    public static final String SPC_TRK_MODULE_NAME_NOOP = "com.linbit.linstor.spacetracking.DefaultSpaceTrackingModule";

    public static final int API_VERSION = 4;
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
    private final ControllerDatabase controllerDb;

    private final DbInitializer dbInitializer;
    private final DbCoreObjProtInitializer dbCoreObjProtInitializer;
    private final DbDataInitializer dbDataInitializer;
    private final DbNumberPoolInitializer dbNumberPoolInitializer;

    private final ApplicationLifecycleManager applicationLifecycleManager;

    // Map of all managed nodes
    private final CoreModule.NodesMap nodesMap;

    private final TaskScheduleService taskScheduleService;
    private final PingTask pingTask;
    private final ReconnectorTask reconnectorTask;
    private final LogArchiveTask logArchiveTask;
    private final AutoSnapshotTask autoSnapshotTask;
    private final AutoDiskfulTask autoDiskfulTask;
    private final BalanceResourcesTask balanceResourcesTask;
    private final UpdateSpaceInfoTask updateSpaceInfoTask;
    private final QsiClearCacheTask qsiClearCache;

    private final DebugConsoleCreator debugConsoleCreator;
    private final ControllerNetComInitializer controllerNetComInitializer;

    private final SpecialSatelliteProcessManager specStltTargetProcessManager;
    private final WhitelistProps whitelistProps;

    private final RetryResourcesTask retryResourcesTask;

    private final CtrlConfig ctrlCfg;

    private final PassphraseInitializer passphraseInitializer;

    private final PreConnectInitializer preConnectCleanupInitializer;
    private final ScheduleBackupService scheduleBackupService;
    private final EbsStatusManagerService ebsStatusManagerService;
    private final ResyncAfterInitializer resyncAfterInitializer;

    @Inject
    public Controller(
        ErrorReporter errorReporterRef,
        @SystemContext
        AccessContext sysCtxRef,
        CoreTimer timerEventSvcRef,
        @Named(CoreModule.RECONFIGURATION_LOCK)
        ReadWriteLock reconfigurationLockRef,
        Map<ServiceName, SystemService> systemServicesMapRef,
        ControllerDatabase controllerDatabaseRef,
        DbInitializer dbConnectionPoolInitializerRef,
        DbCoreObjProtInitializer dbCoreObjProtInitializerRef,
        DbDataInitializer dbDataInitializerRef,
        DbNumberPoolInitializer dbNumberPoolInitializerRef,
        ApplicationLifecycleManager applicationLifecycleManagerRef,
        CoreModule.NodesMap nodesMapRef,
        TaskScheduleService taskScheduleServiceRef,
        PingTask pingTaskRef,
        ReconnectorTask reconnectorTaskRef,
        RetryResourcesTask retryResourcesTaskRef,
        UpdateSpaceInfoTask updateSpaceInfoTaskRef,
        LogArchiveTask logArchiveTaskRef,
        AutoSnapshotTask autoSnapshotTaskRef,
        AutoDiskfulTask autoDiskfulTaskRef,
        BalanceResourcesTask balanceResourcesTaskRef,
        QsiClearCacheTask qsiClearCacheRef,
        DebugConsoleCreator debugConsoleCreatorRef,
        ControllerNetComInitializer controllerNetComInitializerRef,
        SpecialSatelliteProcessManager specialStltTargetProcessManagerRef,
        WhitelistProps whitelistPropsRef,
        CtrlConfig ctrlCfgRef,
        PassphraseInitializer passphraseInitializerRef,
        PreConnectInitializer preConnectCleanupInitializerRef,
        ScheduleBackupService scheduleBackupServiceRef,
        EbsStatusManagerService ebsStatusManagerServiceRef,
        ResyncAfterInitializer resyncAfterInitializerRef
    )
    {
        errorReporter = errorReporterRef;
        sysCtx = sysCtxRef;
        timerEventSvc = timerEventSvcRef;
        reconfigurationLock = reconfigurationLockRef;
        systemServicesMap = systemServicesMapRef;
        controllerDb = controllerDatabaseRef;
        dbInitializer = dbConnectionPoolInitializerRef;
        dbCoreObjProtInitializer = dbCoreObjProtInitializerRef;
        dbDataInitializer = dbDataInitializerRef;
        dbNumberPoolInitializer = dbNumberPoolInitializerRef;
        applicationLifecycleManager = applicationLifecycleManagerRef;
        // Controller configuration properties
        nodesMap = nodesMapRef;
        taskScheduleService = taskScheduleServiceRef;
        pingTask = pingTaskRef;
        reconnectorTask = reconnectorTaskRef;
        logArchiveTask = logArchiveTaskRef;
        retryResourcesTask = retryResourcesTaskRef;
        updateSpaceInfoTask = updateSpaceInfoTaskRef;
        autoSnapshotTask = autoSnapshotTaskRef;
        autoDiskfulTask = autoDiskfulTaskRef;
        balanceResourcesTask = balanceResourcesTaskRef;
        qsiClearCache = qsiClearCacheRef;
        debugConsoleCreator = debugConsoleCreatorRef;
        controllerNetComInitializer = controllerNetComInitializerRef;
        specStltTargetProcessManager = specialStltTargetProcessManagerRef;
        whitelistProps = whitelistPropsRef;
        ctrlCfg = ctrlCfgRef;
        passphraseInitializer = passphraseInitializerRef;
        preConnectCleanupInitializer = preConnectCleanupInitializerRef;
        scheduleBackupService = scheduleBackupServiceRef;
        ebsStatusManagerService = ebsStatusManagerServiceRef;
        resyncAfterInitializer = resyncAfterInitializerRef;
    }

    public void start(Injector injector, CtrlConfig linstorCfgRef)
    {
        applicationLifecycleManager.installShutdownHook();

        reconfigurationLock.writeLock().lock();

        try
        {
            AccessContext initCtx = sysCtx.clone();
            initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            Level tmpLinLevel = null;
            String logLevelLinstorStr = linstorCfgRef.getLogLevelLinstor();
            String logLevelStr = linstorCfgRef.getLogLevel();
            try
            {
                if (logLevelLinstorStr != null)
                {
                    tmpLinLevel = Level.valueOf(logLevelLinstorStr.toUpperCase());
                }
                else
                {
                    tmpLinLevel = Level.valueOf(logLevelStr.toUpperCase());
                }
            }
            catch (IllegalArgumentException exc)
            {
                errorReporter.logError("Invalid Linstor Log level '" + logLevelLinstorStr + "'");
            }

            try
            {
                errorReporter.setLogLevel(
                    initCtx,
                    Level.valueOf(logLevelStr.toUpperCase()),
                    tmpLinLevel
                );
            }
            catch (IllegalArgumentException exc)
            {
                errorReporter.logError("Invalid Log level '" + logLevelStr + "'");
            }

            taskScheduleService.addTask(pingTask);
            taskScheduleService.addTask(reconnectorTask);
            taskScheduleService.addTask(retryResourcesTask);
            taskScheduleService.addTask(logArchiveTask);
            taskScheduleService.addTask(autoSnapshotTask);
            taskScheduleService.addTask(autoDiskfulTask);
            taskScheduleService.addTask(balanceResourcesTask);
            taskScheduleService.addTask(updateSpaceInfoTask);
            taskScheduleService.addTask(qsiClearCache);

            systemServicesMap.put(controllerDb.getInstanceName(), controllerDb);
            systemServicesMap.put(taskScheduleService.getInstanceName(), taskScheduleService);
            systemServicesMap.put(scheduleBackupService.getInstanceName(), scheduleBackupService);
            systemServicesMap.put(timerEventSvc.getInstanceName(), timerEventSvc);
            systemServicesMap.put(ebsStatusManagerService.getInstanceName(), ebsStatusManagerService);
            SystemService spaceTrackingService = null;
            try
            {
                spaceTrackingService = injector.getInstance(
                    Key.get(SystemService.class, Names.named(Controller.SPC_TRK_MODULE_NAME))
                );
                if (spaceTrackingService != null)
                {
                    systemServicesMap.put(spaceTrackingService.getInstanceName(), spaceTrackingService);
                    errorReporter.logInfo("%s", "SpaceTrackingService: Instance added as a system service");
                }
            }
            catch (ConfigurationException | ProvisionException injExc)
            {
                // ignored, debug log will be printed outside of try/catch block
            }
            if (spaceTrackingService == null)
            {
                errorReporter.logDebug("%s", "SpaceTrackingService: No instance available to add as a system service");
            }

            ConnectNodesInitializer connectNodesInitializer = new ConnectNodesInitializer(
                errorReporter,
                nodesMap,
                reconnectorTask,
                initCtx
            );
            GrizzlyInitializer grizzlyInit = new GrizzlyInitializer(
                injector,
                errorReporter,
                ctrlCfg,
                systemServicesMap
            );

            SpecStltProcMgrInit specStltProcessMgrInit = new SpecStltProcMgrInit(
                specStltTargetProcessManager
            );

            ArrayList<StartupInitializer> startOrderlist = new ArrayList<>();

            startOrderlist.add(new ServiceStarter(timerEventSvc));
            startOrderlist.add(dbInitializer);
            startOrderlist.add(new ServiceStarter(controllerDb));
            startOrderlist.add(dbDataInitializer);
            startOrderlist.add(dbNumberPoolInitializer);
            startOrderlist.add(new ServiceStarter(taskScheduleService));
            startOrderlist.add(controllerNetComInitializer);
            if (ctrlCfg.getMasterPassphrase() != null)
            {
                // initialize passphrase before contacting satellites so that a possible passphrase can be included in
                // the FullSync
                startOrderlist.add(passphraseInitializer);
            }
            startOrderlist.add(preConnectCleanupInitializer);
            startOrderlist.add(resyncAfterInitializer);
            startOrderlist.add(connectNodesInitializer);
            startOrderlist.add(specStltProcessMgrInit);
            if (spaceTrackingService != null)
            {
                startOrderlist.add(new ServiceStarter(spaceTrackingService));
            }
            startOrderlist.add(new ServiceStarter(scheduleBackupService));
            startOrderlist.add(new ServiceStarter(ebsStatusManagerService));
            startOrderlist.add(grizzlyInit);

            applicationLifecycleManager.startSystemServices(startOrderlist);

            whitelistProps.overrideDrbdProperties();

            String notifySocket = System.getenv("NOTIFY_SOCKET");
            if (notifySocket != null && !notifySocket.trim().isEmpty())
            {
                Runtime.getRuntime().exec("systemd-notify READY=1");
            }
            else
            {
                errorReporter.logWarning(
                    "Not calling 'systemd-notify' as NOTIFY_SOCKET is %s",
                    notifySocket == null ? "null" : "empty"
                );
            }

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
        catch (SystemServiceStartException | IOException exc)
        {
            errorReporter.reportError(Level.ERROR, exc);
            reconfigurationLock.writeLock().unlock();
            System.exit(InternalApiConsts.EXIT_CODE_NETCOM_ERROR);

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

    public static DatabaseDriverInfo.DatabaseType checkDatabaseConfig(
        ErrorReporter errorReporter,
        CtrlConfig linstorConfig
    )
    {
        DatabaseDriverInfo.DatabaseType dbType;
        String dbConnectionUrl = linstorConfig.getDbConnectionUrl();
        if (dbConnectionUrl.startsWith("jdbc"))
        {
            dbType = DatabaseDriverInfo.DatabaseType.SQL;
        }
        else if (dbConnectionUrl.startsWith("etcd"))
        {
            dbType = DatabaseDriverInfo.DatabaseType.ETCD;
        }
        else if (dbConnectionUrl.startsWith("k8s"))
        {
            dbType = DatabaseDriverInfo.DatabaseType.K8S_CRD;
        }
        else
        {
            errorReporter.logError("Database uri not supported: " + dbConnectionUrl);
            System.exit(InternalApiConsts.EXIT_CODE_CONFIG_PARSE_ERROR);
            throw new RuntimeException("Can't touch this");
        }

        return dbType;
    }

    public static void main(String[] args)
    {
        System.out.printf("%s, Module %s\n", LinStor.PROGRAM, LinStor.CONTROLLER_MODULE);
        LinStor.printStartupInfo();

        CtrlConfig cfg = new CtrlConfig(args);

        System.setProperty("log.module", LinStor.CONTROLLER_MODULE);
        System.setProperty("log.directory", cfg.getLogDirectory());

        Path sentryFilePath = Paths.get(cfg.getConfigDir(), "sentry.properties");
        System.setProperty("sentry.properties.file", sentryFilePath.toString());

        EtcdUtils.linstorPrefix = cfg.getEtcdPrefix().endsWith("/") ? cfg.getEtcdPrefix() : cfg.getEtcdPrefix() + '/';

        MDC.put(ErrorReporter.LOGID, "ffffff");
        StdErrorReporter errorLog = new StdErrorReporter(
            LinStor.CONTROLLER_MODULE,
            Paths.get(cfg.getLogDirectory()),
            cfg.isLogPrintStackTrace(),
            LinStor.getHostName(),
            cfg.getLogLevel(),
            cfg.getLogLevelLinstor(),
            () -> null
        );

        // check database type
        DatabaseDriverInfo.DatabaseType dbType = checkDatabaseConfig(errorLog, cfg);
        errorLog.logInfo("%s %s", "Database type is", dbType.displayName());

        boolean dbgCnsEnabled = false;
        Controller instance = null;
        try
        {
            Thread.currentThread().setName("Main");

            dbgCnsEnabled = cfg.isDebugConsoleEnabled();

            final boolean haveFipsInit = LinStor.initializeFips(errorLog);

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

            /*
             * Dynamic loading is very slow compared to static loading, each .loadClasses
             * costs around ~400ms on my system. so we do it static now, there are only 4 event classes anyway
             *
             * List<Class<? extends EventSerializer>> eventSerializers = classPathLoader.loadClasses(
             *      ProtobufEventSerializer.class.getPackage().getName(),
             *      packageSuffixes,
             *      EventSerializer.class,
             *      ProtobufEventSerializer.class
             * );
             *
             * List<Class<? extends EventHandler>> eventHandlers = classPathLoader.loadClasses(
             *      ProtobufEventHandler.class.getPackage().getName(),
             *      packageSuffixes,
             *      EventHandler.class,
             *      ProtobufEventHandler.class
             * );
             */

            List<Class<? extends EventSerializer>> eventSerializers = Arrays.asList(
                ResourceStateEventSerializer.class,
                VolumeDiskStateEventSerializer.class,
                ReplicationStateEventSerializer.class,
                DonePercentageEventSerializer.class,
                ConnectionStateEventSerializer.class
            );

            List<Class<? extends EventHandler>> eventHandlers = Arrays.asList(
                ResourceStateEventHandler.class,
                VolumeDiskStateEventHandler.class,
                ReplicationStateEventHandler.class,
                DonePercentageEventHandler.class,
                ConnectionStateEventHandler.class
            );
            errorLog.logInfo(
                String.format(
                    "API classes loading finished: %dms",
                    (System.currentTimeMillis() - startAPIClassLoadingTime)
                )
            );

            errorLog.logInfo("Dependency injection started.");
            long startDepInjectionTime = System.currentTimeMillis();

            final List<Module> injModList = new LinkedList<>(
                Arrays.asList(
                    new GuiceConfigModule(),
                    new LoggingModule(errorLog),
                    new SecurityModule(),
                    new ControllerSecurityModule(),
                    new CtrlConfigModule(cfg),
                    new CoreTimerModule(),
                    new MetaDataModule(),
                    new ControllerLinstorModule(),
                    new LinStorModule(),
                    new CoreModule(),
                    new ControllerCoreModule(),
                    new ControllerSatelliteCommunicationModule(),
                    new ControllerDbModule(dbType),
                    new NetComModule(),
                    new NumberPoolModule(),
                    new ApiModule(apiType, apiCalls),
                    new ApiCallHandlerModule(),
                    new CtrlApiCallHandlerModule(),
                    new EventModule(eventSerializers, eventHandlers),
                    new DebugModule(),
                    new ControllerDebugModule(),
                    new ControllerTransactionMgrModule(dbType),
                    new NameShortenerModule(),
                    new LayerSizeCalculatorModule()
                )
            );
            LinStor.loadModularCrypto(injModList, errorLog, haveFipsInit);
            if (!InjectorLoader.dynLoadInjModule(SPC_TRK_MODULE_NAME, injModList, errorLog, dbType))
            {
                InjectorLoader.dynLoadInjModule(SPC_TRK_MODULE_NAME_NOOP, injModList, errorLog, dbType);
            }
            final Injector injector = Guice.createInjector(injModList);

            errorLog.logInfo(
                String.format(
                    "Dependency injection finished: %dms",
                    (System.currentTimeMillis() - startDepInjectionTime)
                )
            );

            {
                ModularCryptoProvider cryptoProvider = injector.getInstance(ModularCryptoProvider.class);
                AbsMigration.setModularCryptoProvider(cryptoProvider);
                errorLog.logInfo("Cryptography provider: Using %s", cryptoProvider.getModuleIdentifier());
            }

            instance = injector.getInstance(Controller.class);
            instance.start(injector, cfg);

            if (dbgCnsEnabled)
            {
                instance.enterDebugConsole();
            }
        }
        catch (Throwable error)
        {
            errorLog.reportError(error);
            // If enabled, attempt to enter the debug console
            if (dbgCnsEnabled)
            {
                if (instance != null)
                {
                    try
                    {
                        instance.enterDebugConsole();
                    }
                    catch (Throwable dbgError)
                    {
                        errorLog.reportError(dbgError);
                    }
                }
            }
            System.exit(InternalApiConsts.EXIT_CODE_IMPL_ERROR);
        }

        System.out.println();
    }
}
