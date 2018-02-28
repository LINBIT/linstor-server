package com.linbit.linstor.core;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.linbit.ImplementationError;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.protobuf.ProtobufApiType;
import com.linbit.linstor.debug.DebugConsole;
import com.linbit.linstor.debug.DebugConsoleCreator;
import com.linbit.linstor.debug.DebugConsoleImpl;
import com.linbit.linstor.drbdstate.DrbdEventService;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Initializer;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.timer.CoreTimer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * linstor satellite prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Satellite extends LinStor
{
    // System module information
    public static final String MODULE = "Satellite";

    private final Injector injector;

    // System security context
    private AccessContext sysCtx;

    // ============================================================
    // Core system services
    //
    // Map of controllable system services
    private Map<ServiceName, SystemService> systemServicesMap;

    // Device manager
    private DeviceManagerImpl devMgr = null;

    private ApplicationLifecycleManager applicationLifecycleManager;

    // Lock for major global changes
    public ReadWriteLock stltConfLock;

    private DebugConsoleCreator debugConsoleCreator;

    public Satellite(
        Injector injectorRef
    )
    {
        injector = injectorRef;
    }

    public void initialize()
    {
        sysCtx = injector.getInstance(Key.get(AccessContext.class, SystemContext.class));

        reconfigurationLock = injector.getInstance(
            Key.get(ReadWriteLock.class, Names.named(CoreModule.RECONFIGURATION_LOCK)));
        stltConfLock = injector.getInstance(
            Key.get(ReadWriteLock.class, Names.named(SatelliteCoreModule.STLT_CONF_LOCK)));

        reconfigurationLock.writeLock().lock();

        try
        {
            AccessContext initCtx = sysCtx.clone();
            initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            errorReporter = injector.getInstance(ErrorReporter.class);

            systemServicesMap = injector.getInstance(Key.get(new TypeLiteral<Map<ServiceName, SystemService>>() {}));

            FileSystemWatch fsWatchSvc = injector.getInstance(FileSystemWatch.class);
            systemServicesMap.put(fsWatchSvc.getInstanceName(), fsWatchSvc);

            timerEventSvc = injector.getInstance(CoreTimer.class);
            systemServicesMap.put(timerEventSvc.getInstanceName(), timerEventSvc);

            applicationLifecycleManager = injector.getInstance(ApplicationLifecycleManager.class);

            debugConsoleCreator = injector.getInstance(DebugConsoleCreator.class);

            errorReporter.logInfo("Initializing StateTracker");
            {
                DrbdEventService drbdEventSvc = injector.getInstance(DrbdEventService.class);

                systemServicesMap.put(drbdEventSvc.getInstanceName(), drbdEventSvc);
            }

            errorReporter.logInfo("Initializing device manager");
            devMgr = injector.getInstance(DeviceManagerImpl.class);
            systemServicesMap.put(devMgr.getInstanceName(), devMgr);

            // Initialize system services
            applicationLifecycleManager.startSystemServices(systemServicesMap.values());

            // Initialize the network communications service
            errorReporter.logInfo("Initializing main network communications service");
            injector.getInstance(SatelliteNetComInitializer.class).initMainNetComService(initCtx);
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

    public static void main(String[] args)
    {
        LinStorArguments cArgs = LinStorArgumentParser.parseCommandLine(args);

        System.out.printf(
            "%s, Module %s\n",
            Satellite.PROGRAM, Satellite.MODULE
        );
        printStartupInfo();

        ErrorReporter errorLog = new StdErrorReporter(Satellite.MODULE, "");

        try
        {
            Thread.currentThread().setName("Main");

            ApiType apiType = new ProtobufApiType();
            List<Class<? extends ApiCall>> apiCalls =
                new ApiCallLoader(errorLog).loadApiCalls(apiType, Arrays.asList("common", "satellite"));

            // Initialize the Satellite module with the SYSTEM security context
            Initializer sysInit = new Initializer();
            Satellite instance = sysInit.initSatellite(cArgs, errorLog, apiType, apiCalls);

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
}
