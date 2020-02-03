package com.linbit.linstor.core;

import com.linbit.GuiceConfigModule;
import com.linbit.ImplementationError;
import com.linbit.SatelliteLinstorModule;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.drbd.DrbdVersion;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.BaseApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiType;
import com.linbit.linstor.core.apicallhandler.ApiCallHandlerModule;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.core.devmgr.DevMgrModule;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.SatelliteDbModule;
import com.linbit.linstor.debug.DebugConsole;
import com.linbit.linstor.debug.DebugConsoleCreator;
import com.linbit.linstor.debug.DebugConsoleImpl;
import com.linbit.linstor.debug.DebugModule;
import com.linbit.linstor.debug.SatelliteDebugModule;
import com.linbit.linstor.drbdstate.DrbdEventPublisher;
import com.linbit.linstor.drbdstate.DrbdEventService;
import com.linbit.linstor.drbdstate.DrbdStateModule;
import com.linbit.linstor.event.EventModule;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.common.ResourceStateEventSerializer;
import com.linbit.linstor.event.serializer.protobuf.common.VolumeDiskStateEventSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.numberpool.SatelliteNumberPoolModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SatelliteSecurityModule;
import com.linbit.linstor.security.SecurityModule;
import com.linbit.linstor.security.StltCoreObjProtInitializer;
import com.linbit.linstor.timer.CoreTimer;
import com.linbit.linstor.timer.CoreTimerModule;
import com.linbit.linstor.transaction.SatelliteTransactionMgrModule;

import javax.inject.Inject;
import javax.inject.Named;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.event.Level;

/**
 * linstor satellite prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Satellite
{
    // Error & exception logging facility
    private final ErrorReporter errorReporter;

    // System security context
    private final AccessContext sysCtx;

    private final CoreTimer timerEventSvc;

    // Synchronization lock for major global changes
    private final ReadWriteLock reconfigurationLock;

    // Map of controllable system services
    private final Map<ServiceName, SystemService> systemServicesMap;

    private final DeviceManager devMgr;

    private final DrbdEventPublisher drbdEventPublisher;

    private final ApplicationLifecycleManager applicationLifecycleManager;

    private final DebugConsoleCreator debugConsoleCreator;

    private final FileSystemWatch fsWatchSvc;

    private final DrbdEventService drbdEventSvc;

    private final SatelliteNetComInitializer satelliteNetComInitializer;

    private final StltCoreObjProtInitializer stltCoreObjProtInitializer;

    private final StltConfig stltCfg;

    @Inject
    public Satellite(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext sysCtxRef,
        CoreTimer timerEventSvcRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        Map<ServiceName, SystemService> systemServicesMapRef,
        DeviceManager devMgrRef,
        DrbdEventPublisher drbdEventPublisherRef,
        ApplicationLifecycleManager applicationLifecycleManagerRef,
        DebugConsoleCreator debugConsoleCreatorRef,
        FileSystemWatch fsWatchSvcRef,
        DrbdEventService drbdEventSvcRef,
        SatelliteNetComInitializer satelliteNetComInitializerRef,
        StltCoreObjProtInitializer stltCoreObjProtInitializerRef,
        StltConfig stltCfgRef
    )
    {
        errorReporter = errorReporterRef;
        sysCtx = sysCtxRef;
        timerEventSvc = timerEventSvcRef;
        reconfigurationLock = reconfigurationLockRef;
        systemServicesMap = systemServicesMapRef;
        devMgr = devMgrRef;
        drbdEventPublisher = drbdEventPublisherRef;
        applicationLifecycleManager = applicationLifecycleManagerRef;
        debugConsoleCreator = debugConsoleCreatorRef;
        fsWatchSvc = fsWatchSvcRef;
        drbdEventSvc = drbdEventSvcRef;
        satelliteNetComInitializer = satelliteNetComInitializerRef;
        stltCoreObjProtInitializer = stltCoreObjProtInitializerRef;
        stltCfg = stltCfgRef;
    }

    public void start()
    {
        applicationLifecycleManager.installShutdownHook();

        reconfigurationLock.writeLock().lock();

        try
        {
            try
            {
                errorReporter.setLogLevel(
                    sysCtx,
                    Level.valueOf(stltCfg.getLogLevel().toUpperCase()),
                    Level.valueOf(stltCfg.getLinstorLogLevel().toUpperCase())
                );
            }
            catch (IllegalArgumentException exc)
            {
                errorReporter.logError("Invalid Log level '" + stltCfg.getLogLevel() + "'");
            }

            try
            {
                // make sure /var/lib/linstor exists
                Files.createDirectories(Paths.get("/var/lib/linstor"));
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(ioExc);
            }

            DrbdVersion vsnCheck = new DrbdVersion(timerEventSvc, errorReporter);
            vsnCheck.checkVersion();
            if (vsnCheck.hasDrbd9())
            {
                ensureDrbdConfigSetup();
            }

            AccessContext initCtx = sysCtx.clone();
            initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            systemServicesMap.put(fsWatchSvc.getInstanceName(), fsWatchSvc);
            systemServicesMap.put(timerEventSvc.getInstanceName(), timerEventSvc);
            if (vsnCheck.hasDrbd9())
            {
                systemServicesMap.put(drbdEventSvc.getInstanceName(), drbdEventSvc);
                systemServicesMap.put(drbdEventPublisher.getInstanceName(), drbdEventPublisher);
            }
            SystemService devMgrService = (SystemService) devMgr;
            systemServicesMap.put(devMgrService.getInstanceName(), devMgrService);

            stltCoreObjProtInitializer.initialize();

            applicationLifecycleManager.startSystemServices(systemServicesMap.values());

            errorReporter.logInfo("Initializing main network communications service");
            if (!satelliteNetComInitializer.initMainNetComService(initCtx))
            {
                reconfigurationLock.writeLock().unlock();
                System.exit(InternalApiConsts.EXIT_CODE_NETCOM_ERROR);
            }
        }
        catch (AccessDeniedException accessExc)
        {
            throw new ImplementationError(
                "The initialization security context does not have all privileges. " +
                    "Initialization failed.",
                accessExc
            );
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        finally
        {
            reconfigurationLock.writeLock().unlock();
        }
    }

    /**
     * Adds /var/lib/drbd.d/ include to drbd.conf and ensures the /var/lib/drbd.d
     * directory exists and removes any *.res file from it excluding .res files
     * matching the regex from the "--keep-res" command line argument
     */
    private void ensureDrbdConfigSetup()
    {
        try
        {
            Path varDrbdPath = Paths.get(CoreModule.CONFIG_PATH);
            Files.createDirectories(varDrbdPath);
            Files.createDirectories(Paths.get(CoreModule.BACKUP_PATH));

            final Pattern keepResPattern = stltCfg.getDrbdKeepResPattern();
            Function<Path, Boolean> keepFunc;
            if (keepResPattern != null)
            {
                errorReporter.logInfo("Removing res files from " + varDrbdPath + ", keeping files matching regex: " +
                    keepResPattern.pattern()
                );
                keepFunc = (path) -> keepResPattern.matcher(path.getFileName().toString()).find();
            }
            else
            {
                errorReporter.logInfo("Removing all res files from " + varDrbdPath);
                keepFunc = (path) -> false;
            }
            try (Stream<Path> files = Files.list(varDrbdPath))
            {
                files.filter(path -> path.toString().endsWith(".res")).forEach(
                    filteredPathName ->
                    {
                        try
                        {
                            if (!keepFunc.apply(filteredPathName))
                            {
                                Files.delete(filteredPathName);
                            }
                        }
                        catch (IOException ioExc)
                        {
                            throw new ImplementationError(
                                "Unable to delete drbd resource file: " + filteredPathName,
                                ioExc
                            );
                        }
                    }
                );
            }
        }
        catch (IOException ioExc)
        {
            throw new ImplementationError(
                "Unable to create linstor drbd configuration directory: " + CoreModule.CONFIG_PATH,
                ioExc
            );
        }

        // now check that the config include is in /etc/drbd.conf
        final Path drbdD = Paths.get("/etc/drbd.d");

        // if /etc/drbd.d doesn't exist, drbd is probably not even installed, so skip
        if (Files.exists(drbdD))
        {
            final Path linstorInclude = Paths.get("/etc/drbd.d/linstor-resources.res");
            if (!Files.exists(linstorInclude))
            {
                try
                {
                    final String text = String.format(
                        "# This line was generated by linstor\n" + "include \"%s/*.res\";\n",
                        CoreModule.CONFIG_PATH
                    );
                    Files.write(linstorInclude, text.getBytes());
                }
                catch (IOException ioExc)
                {
                    throw new ImplementationError(
                        "Unable to write linstor drbd include file to: " + linstorInclude.toString(),
                        ioExc
                    );
                }
            }
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

    public static void main(String[] args)
    {
        StltConfig cfg = new StltConfig(args);

        System.setProperty("log.module", LinStor.SATELLITE_MODULE);
        System.setProperty("log.directory", cfg.getLogDirectory());

        System.out.printf("%s, Module %s\n", LinStor.PROGRAM, LinStor.SATELLITE_MODULE);
        LinStor.printStartupInfo();

        StdErrorReporter errorLog = new StdErrorReporter(
            LinStor.SATELLITE_MODULE,
            Paths.get(cfg.getLogDirectory()),
            cfg.isLogPrintStackTrace(),
            cfg.getStltOverrideNodeName() != null ? cfg.getStltOverrideNodeName() : LinStor.getHostName(),
            cfg.getLogLevel(),
            cfg.getLinstorLogLevel(),
            () -> null
        );

        try
        {
            Thread.currentThread().setName("Main");

            errorLog.logInfo("Loading API classes started.");
            long startAPIClassLoadingTime = System.currentTimeMillis();
            ApiType apiType = new ProtobufApiType();
            ClassPathLoader classPathLoader = new ClassPathLoader(errorLog);
            List<String> packageSuffixes = Arrays.asList("common", "satellite");

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
             */

            List<Class<? extends EventSerializer>> eventSerializers = Arrays.asList(
                ResourceStateEventSerializer.class,
                VolumeDiskStateEventSerializer.class
            );

            errorLog.logInfo(
                String.format(
                    "API classes loading finished: %dms",
                    (System.currentTimeMillis() - startAPIClassLoadingTime)
                )
            );

            errorLog.logInfo("Dependency injection started.");
            long startDepInjectionTime = System.currentTimeMillis();
            Injector injector = Guice.createInjector(
                new GuiceConfigModule(),
                new LoggingModule(errorLog),
                new SecurityModule(),
                new SatelliteSecurityModule(),
                new SatelliteArgumentsModule(cfg),
                new CoreTimerModule(),
                new SatelliteLinstorModule(),
                new LinStorModule(),
                new CoreModule(),
                new SatelliteCoreModule(),
                new DevMgrModule(),
                new SatelliteDbModule(),
                new DrbdStateModule(),
                new ApiModule(apiType, apiCalls),
                new ApiCallHandlerModule(),
                new EventModule(eventSerializers, Collections.emptyList()),
                new DebugModule(),
                new SatelliteDebugModule(),
                new SatelliteTransactionMgrModule(),
                new SatelliteNumberPoolModule()
            );
            errorLog.logInfo(
                String.format(
                    "Dependency injection finished: %dms",
                    (System.currentTimeMillis() - startDepInjectionTime)
                )
            );

            Satellite instance = injector.getInstance(Satellite.class);
            instance.start();

            if (cfg.isDebugConsoleEnabled())
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
