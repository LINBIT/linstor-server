package com.linbit.linstor.core;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
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
import com.linbit.GuiceConfigModule;
import com.linbit.ImplementationError;
import com.linbit.SatelliteLinstorModule;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.drbd.DrbdVersion;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
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
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.ProtobufEventSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SatelliteSecurityModule;
import com.linbit.linstor.security.SecurityModule;
import com.linbit.linstor.security.StltCoreObjProtInitializer;
import com.linbit.linstor.timer.CoreTimer;
import com.linbit.linstor.timer.CoreTimerModule;
import com.linbit.linstor.transaction.SatelliteTransactionMgrModule;
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

    private final DeviceManagerImpl devMgr;

    private final DrbdEventPublisher drbdEventPublisher;

    private final ApplicationLifecycleManager applicationLifecycleManager;

    private final DebugConsoleCreator debugConsoleCreator;

    private final FileSystemWatch fsWatchSvc;

    private final DrbdEventService drbdEventSvc;

    private final SatelliteNetComInitializer satelliteNetComInitializer;

    private final SatelliteCmdlArguments satelliteCmdlArguments;

    private static boolean skipHostnameCheck;

    private final StltCoreObjProtInitializer stltCoreObjProtInitializer;

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
        SatelliteCmdlArguments satelliteCmdlArgumentsRef,
        StltCoreObjProtInitializer stltCoreObjProtInitializerRef
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
        satelliteCmdlArguments = satelliteCmdlArgumentsRef;
        stltCoreObjProtInitializer = stltCoreObjProtInitializerRef;
    }

    public void start()
    {
        applicationLifecycleManager.installShutdownHook();

        reconfigurationLock.writeLock().lock();

        try
        {
            skipHostnameCheck = satelliteCmdlArguments.isSkipHostnameCheck();

            if (!satelliteCmdlArguments.isSkipDrbdCheck())
            {
                DrbdVersion vsnCheck = new DrbdVersion(timerEventSvc, errorReporter);
                if (!vsnCheck.isDrbd9())
                {
                    errorReporter.reportProblem(
                        Level.ERROR,
                        new LinStorException(
                            "Satellite startup failed, unable to verify the presence of a supported DRBD installation",
                            "Satellite startup failed",
                            LinStor.PROGRAM + " could not verify that a supported version of DRBD is installed\n" +
                            "on this system",
                            "- Ensure that DRBD is installed and accessible by " + LinStor.PROGRAM + "\n" +
                            "- " + LinStor.PROGRAM + " requires DRBD version 9. DRBD version 8 is NOT supported.\n" +
                            "- If you intend to run " + LinStor.PROGRAM + " without using DRBD, refer to\n" +
                            "  product documentation or command line options help for information on how\n" +
                            "  to skip the check for a supported DRBD installation",
                            null
                        ),
                        null, null, null
                    );
                    reconfigurationLock.writeLock().unlock();
                    System.exit(InternalApiConsts.EXIT_CODE_DRBD_ERROR);
                }
            }

            ensureDrbdConfigSetup();

            AccessContext initCtx = sysCtx.clone();
            initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            systemServicesMap.put(fsWatchSvc.getInstanceName(), fsWatchSvc);
            systemServicesMap.put(timerEventSvc.getInstanceName(), timerEventSvc);
            systemServicesMap.put(drbdEventSvc.getInstanceName(), drbdEventSvc);
            systemServicesMap.put(devMgr.getInstanceName(), devMgr);
            systemServicesMap.put(drbdEventPublisher.getInstanceName(), drbdEventPublisher);

            stltCoreObjProtInitializer.initialize();

            applicationLifecycleManager.startSystemServices(systemServicesMap.values());

            errorReporter.logInfo("Initializing main network communications service");
            if (!satelliteNetComInitializer.initMainNetComService(
                initCtx,
                Paths.get(satelliteCmdlArguments.getConfigurationDirectory()),
                satelliteCmdlArguments.getBindAddress(),
                satelliteCmdlArguments.getPlainPortOverride())
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
        catch (SQLException exc)
        {
            throw new ImplementationError(exc);
        }
        finally
        {
            reconfigurationLock.writeLock().unlock();
        }
    }

    /**
     * Adds /var/lib/drbd.d/ include to drbd.conf and ensures the /var/lib/drbd.d directory exists
     * and removes any *.res file from it excluding .res files matching the regex from the
     * "--keep-res" command line argument
     */
    private void ensureDrbdConfigSetup()
    {
        try
        {
            Path varDrbdPath = Paths.get(SatelliteCoreModule.CONFIG_PATH);
            Files.createDirectories(varDrbdPath);
            final Pattern keepResPattern = satelliteCmdlArguments.getKeepResPattern();
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
                final String text = String.format(
                    "# This line was generated by linstor\n" +
                        "include \"%s/*.res\";\n",
                    SatelliteCoreModule.CONFIG_PATH);
                Files.write(linstorInclude, text.getBytes());
            } catch (IOException ioExc)
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

    public static boolean checkHostname()
    {
        return !skipHostnameCheck;
    }

    public static void main(String[] args)
    {
        SatelliteCmdlArguments cArgs = SatelliteArgumentParser.parseCommandLine(args);

        System.setProperty("log.module", LinStor.SATELLITE_MODULE);
        System.setProperty("log.directory", cArgs.getLogDirectory());

        System.out.printf(
            "%s, Module %s\n",
            LinStor.PROGRAM, LinStor.SATELLITE_MODULE
        );
        LinStor.printStartupInfo();

        ErrorReporter errorLog = new StdErrorReporter(
            LinStor.SATELLITE_MODULE,
            Paths.get(cArgs.getLogDirectory()),
            cArgs.isPrintStacktraces(),
            cArgs.getOverrideNodeName() != null ? cArgs.getOverrideNodeName() : LinStor.getHostName()
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

            List<Class<? extends EventSerializer>> eventSerializers = classPathLoader.loadClasses(
                ProtobufEventSerializer.class.getPackage().getName(),
                packageSuffixes,
                EventSerializer.class,
                ProtobufEventSerializer.class
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
                new SatelliteArgumentsModule(cArgs),
                new CoreTimerModule(),
                new SatelliteLinstorModule(),
                new LinStorModule(),
                new CoreModule(),
                new SatelliteCoreModule(),
                new SatelliteDbModule(),
                new DrbdStateModule(),
                new ApiModule(apiType, apiCalls),
                new ApiCallHandlerModule(),
                new EventModule(eventSerializers, Collections.emptyList()),
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
            System.exit(InternalApiConsts.EXIT_CODE_IMPL_ERROR);
        }

        System.out.println();
    }
}
