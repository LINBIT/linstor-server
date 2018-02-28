package com.linbit.linstor.core;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.protobuf.ProtobufApiType;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.debug.DebugConsole;
import com.linbit.linstor.debug.DebugConsoleCreator;
import com.linbit.linstor.debug.DebugConsoleImpl;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.DbAccessor;
import com.linbit.linstor.security.Initializer;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SecurityLevel;
import com.linbit.linstor.tasks.GarbageCollectorTask;
import com.linbit.linstor.tasks.PingTask;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.linstor.tasks.TaskScheduleService;
import com.linbit.linstor.timer.CoreTimer;

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
public final class Controller extends LinStor
{
    // System module information
    public static final String MODULE = "Controller";

    private static final String PROPSCON_KEY_NETCOM = "netcom";

    public static final int API_VERSION = 1;
    public static final int API_MIN_VERSION = API_VERSION;

    public static final Pattern RANGE_PATTERN = Pattern.compile("(?<min>\\d+) ?- ?(?<max>\\d+)");

    private final Injector injector;

    // System security context
    private AccessContext sysCtx;

    // ============================================================
    // Core system services
    //
    // Map of controllable system services
    private Map<ServiceName, SystemService> systemServicesMap;

    // Database connection pool service
    private DbConnectionPool dbConnPool;

    // Satellite reconnector service
    private TaskScheduleService taskScheduleService;

    private ApplicationLifecycleManager applicationLifecycleManager;

    // Controller configuration properties
    private Props ctrlConf;

    // Map of all managed nodes
    Map<NodeName, Node> nodesMap;

    private ReconnectorTask reconnectorTask;

    private DebugConsoleCreator debugConsoleCreator;

    public Controller(
        Injector injectorRef
    )
    {
        injector = injectorRef;
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

            ApiType apiType = new ProtobufApiType();
            List<Class<? extends ApiCall>> apiCalls =
                new ApiCallLoader(errorLog).loadApiCalls(apiType, Arrays.asList("common", "controller"));

            // Initialize the Controller module with the SYSTEM security context
            Initializer sysInit = new Initializer();
            Controller instance = sysInit.initController(cArgs, errorLog, apiType, apiCalls);

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
        sysCtx = injector.getInstance(Key.get(AccessContext.class, SystemContext.class));

        reconfigurationLock = injector.getInstance(
            Key.get(ReadWriteLock.class, Names.named(CoreModule.RECONFIGURATION_LOCK)));

        reconfigurationLock.writeLock().lock();

        try
        {
            AccessContext initCtx = sysCtx.clone();
            initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            errorReporter = injector.getInstance(ErrorReporter.class);

            systemServicesMap = injector.getInstance(Key.get(new TypeLiteral<Map<ServiceName, SystemService>>() {}));

            dbConnPool = injector.getInstance(DbConnectionPool.class);
            systemServicesMap.put(dbConnPool.getInstanceName(), dbConnPool);

            // Object protection loading has a hidden dependency on initializing the security objects
            // (via com.linbit.linstor.security.Role.GLOBAL_ROLE_MAP)
            initializeSecurityObjects(errorReporter, initCtx);

            taskScheduleService = new TaskScheduleService(errorReporter);
            systemServicesMap.put(taskScheduleService.getInstanceName(), taskScheduleService);

            timerEventSvc = injector.getInstance(CoreTimer.class);
            systemServicesMap.put(timerEventSvc.getInstanceName(), timerEventSvc);

            nodesMap = injector.getInstance(CoreModule.NodesMap.class);

            ctrlConf = injector.getInstance(Key.get(Props.class, Names.named(ControllerCoreModule.CONTROLLER_PROPS)));

            applicationLifecycleManager = injector.getInstance(ApplicationLifecycleManager.class);

            // Initialize tasks
            reconnectorTask = injector.getInstance(ReconnectorTask.class);
            PingTask pingTask = injector.getInstance(PingTask.class);
            taskScheduleService.addTask(pingTask);
            taskScheduleService.addTask(reconnectorTask);

            taskScheduleService.addTask(new GarbageCollectorTask());

            debugConsoleCreator = injector.getInstance(DebugConsoleCreator.class);

            injector.getInstance(ControllerNetComInitializer.class).initNetComServices(
                ctrlConf.getNamespace(PROPSCON_KEY_NETCOM),
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

    private void initializeSecurityObjects(final ErrorReporter errorLogRef, final AccessContext initCtx)
        throws InitializationException
    {
        // Load security identities, roles, domains/types, etc.
        errorLogRef.logInfo("Loading security objects");
        try
        {
            Initializer.load(initCtx, dbConnPool, injector.getInstance(DbAccessor.class));
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
