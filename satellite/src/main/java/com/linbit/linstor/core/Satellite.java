package com.linbit.linstor.core;

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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.linbit.GuiceConfigModule;
import com.linbit.ImplementationError;
import com.linbit.SatelliteLinstorModule;
import com.linbit.ServiceName;
import com.linbit.SystemService;
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
import com.linbit.linstor.event.SatelliteEventModule;
import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.event.writer.protobuf.ProtobufEventWriter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SatelliteSecurityModule;
import com.linbit.linstor.security.SecurityModule;
import com.linbit.linstor.timer.CoreTimer;
import com.linbit.linstor.timer.CoreTimerModule;
import com.linbit.linstor.transaction.SatelliteTransactionMgrModule;

/**
 * linstor satellite prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Satellite
{
    // System module information
    public static final String MODULE = "Satellite";

    // Error & exception logging facility
    private final ErrorReporter errorReporter;

    // System security context
    private final AccessContext sysCtx;

    private final CoreTimer timerEventSvc;

    // Synchronization lock for major global changes
    private final ReadWriteLock reconfigurationLock;

    // Map of controllable system services
    private final Map<ServiceName, SystemService> systemServicesMap;

    private final DeviceManagerImpl devMgr;

    private final DrbdEventPublisher drbdEventPublisher;

    private final ApplicationLifecycleManager applicationLifecycleManager;

    private final DebugConsoleCreator debugConsoleCreator;

    private final FileSystemWatch fsWatchSvc;

    private final DrbdEventService drbdEventSvc;

    private final SatelliteNetComInitializer satelliteNetComInitializer;

    private final LinStorArguments linStorArguments;

    @Inject
    public Satellite(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext sysCtxRef,
        CoreTimer timerEventSvcRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        Map<ServiceName, SystemService> systemServicesMapRef,
        DeviceManagerImpl devMgrRef,
        DrbdEventPublisher drbdEventPublisherRef,
        ApplicationLifecycleManager applicationLifecycleManagerRef,
        DebugConsoleCreator debugConsoleCreatorRef,
        FileSystemWatch fsWatchSvcRef,
        DrbdEventService drbdEventSvcRef,
        SatelliteNetComInitializer satelliteNetComInitializerRef,
        LinStorArguments linStorArgumentsRef
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
        linStorArguments = linStorArgumentsRef;
    }

    public void start()
    {
        applicationLifecycleManager.installShutdownHook();

        reconfigurationLock.writeLock().lock();

        try
        {
            ensureDrbdConfigSetup();

            AccessContext initCtx = sysCtx.clone();
            initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            systemServicesMap.put(fsWatchSvc.getInstanceName(), fsWatchSvc);
            systemServicesMap.put(timerEventSvc.getInstanceName(), timerEventSvc);
            systemServicesMap.put(drbdEventSvc.getInstanceName(), drbdEventSvc);
            systemServicesMap.put(devMgr.getInstanceName(), devMgr);
            systemServicesMap.put(drbdEventPublisher.getInstanceName(), drbdEventPublisher);

            applicationLifecycleManager.startSystemServices(systemServicesMap.values());

            errorReporter.logInfo("Initializing main network communications service");
            if (!satelliteNetComInitializer.initMainNetComService(
                initCtx,
                Paths.get(linStorArguments.getConfigurationDirectory()))
            )
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
        finally
        {
            reconfigurationLock.writeLock().unlock();
        }
    }

    /**
     * Adds /var/lib/drbd.d/ include to drbd.conf and ensures the /var/lib/drbd.d directory exists
     * and removes any *.res file from it
     */
    private void ensureDrbdConfigSetup()
    {
        try
        {
            Path varDrbdPath = Paths.get(SatelliteCoreModule.CONFIG_PATH);
            Files.createDirectories(varDrbdPath);
            errorReporter.logInfo("Removing res files from " + varDrbdPath);
            Files.list(varDrbdPath).filter(p -> p.toString().endsWith(".res")).forEach(p ->
            {
                try
                {
                    Files.delete(p);
                }
                catch (IOException ioExc)
                {
                    throw new ImplementationError(
                        "Unable to delete drbd resource file: " + p,
                        ioExc
                    );
                }
            });
        }
        catch (IOException ioExc)
        {
            throw new ImplementationError(
                "Unable to create linstor drbd configuration directory: " + SatelliteCoreModule.CONFIG_PATH,
                ioExc
            );
        }

        // now check that the config include is in /etc/drbd.conf
        final Path linstorInclude = Paths.get("/etc/drbd.d/linstor-resources.res");
        if (!Files.exists(linstorInclude))
        {
            try
            {
                final String text = "# This line was added by linstor\n" +
                    "include \"/var/lib/drbd.d/*.res\";\n";
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
        System.setProperty("log.module", MODULE);

        LinStorArguments cArgs = LinStorArgumentParser.parseCommandLine(args);

        System.out.printf(
            "%s, Module %s\n",
            LinStor.PROGRAM, Satellite.MODULE
        );
        LinStor.printStartupInfo();

        ErrorReporter errorLog = new StdErrorReporter(
            Satellite.MODULE,
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
            List<String> packageSuffixes = Arrays.asList("common", "satellite");

            List<Class<? extends BaseApiCall>> apiCalls = classPathLoader.loadClasses(
                ProtobufApiType.class.getPackage().getName(),
                packageSuffixes,
                BaseApiCall.class,
                ProtobufApiCall.class
            );

            List<Class<? extends EventWriter>> eventWriters = classPathLoader.loadClasses(
                ProtobufEventWriter.class.getPackage().getName(),
                packageSuffixes,
                EventWriter.class,
                ProtobufEventWriter.class
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
                new SatelliteSecurityModule(),
                new LinStorArgumentsModule(cArgs),
                new CoreTimerModule(),
                new SatelliteLinstorModule(),
                new LinStorModule(),
                new CoreModule(),
                new SatelliteCoreModule(),
                new SatelliteDbModule(),
                new DrbdStateModule(),
                new ApiModule(apiType, apiCalls),
                new ApiCallHandlerModule(),
                new EventModule(eventWriters, Collections.emptyList()),
                new SatelliteEventModule(),
                new DebugModule(),
                new SatelliteDebugModule(),
                new SatelliteTransactionMgrModule()
            );
            errorLog.logInfo(String.format(
                "Dependency injection finished: %dms",
                (System.currentTimeMillis() - startDepInjectionTime))
            );

            Satellite instance = injector.getInstance(Satellite.class);
            instance.start();

            if (cArgs.startDebugConsole())
            {
                instance.enterDebugConsole();
            }
        }
        catch (Throwable error)
        {
            errorLog.reportError(error);
            System.exit(1);
        }

        System.out.println();
    }
}
