package com.linbit.drbdmanage;

import com.linbit.InvalidNameException;
import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.WorkerPool;
import com.linbit.drbdmanage.controllerapi.Ping;
import com.linbit.drbdmanage.debug.ControllerDebugCmd;
import com.linbit.drbdmanage.debug.DebugErrorReporter;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnectionObserver;
import com.linbit.drbdmanage.netcom.TcpConnectorService;
import com.linbit.drbdmanage.proto.CommonMessageProcessor;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.Identity;
import com.linbit.drbdmanage.security.IdentityName;
import com.linbit.drbdmanage.security.Initializer;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.Privilege;
import com.linbit.drbdmanage.security.PrivilegeSet;
import com.linbit.drbdmanage.security.Role;
import com.linbit.drbdmanage.security.RoleName;
import com.linbit.drbdmanage.security.SecTypeName;
import com.linbit.drbdmanage.security.SecurityType;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.timer.Action;
import com.linbit.timer.GenericTimer;
import com.linbit.timer.Timer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * drbdmanageNG controller prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class Controller implements Runnable, CoreServices
{
    public static final String PROGRAM = "drbdmanageNG";
    public static final String MODULE = "Controller";
    public static final String VERSION = "experimental 2017-03-10_001";

    public static final int MIN_WORKER_QUEUE_SIZE = 32;
    public static final int MAX_CPU_COUNT = 1024;

    // At shutdown, wait at most SHUTDOWN_THR_JOIN_WAIT milliseconds for
    // a service thread to end
    public static final long SHUTDOWN_THR_JOIN_WAIT = 3000L;

    public static final IdentityName ID_ANON_CLIENT_NAME;
    public static final RoleName ROLE_PUBLIC_NAME;
    public static final SecTypeName TYPE_PUBLIC_NAME;

    // Defaults
    private int cpuCount = 8;
    private int workerThreadCount = 8;
    // Queue slots per worker thread
    private int workerQueueFactor = 4;
    private int workerQueueSize = MIN_WORKER_QUEUE_SIZE;

    public static final String SCREEN_DIV =
        "------------------------------------------------------------------------------";

    // System security context
    private final AccessContext sysCtx;
    // Default security identity for unauthenticated clients
    private final Identity      anonClientId;
    // Public access security role
    private final Role          publicRole;
    // Public access security type
    private final SecurityType  publicType;

    private String[] args;

    private final GenericTimer<String, Action<String>> timerEventSvc;
    private final FileSystemWatch fsEventSvc;

    // Map of controllable system services
    private final Map<ServiceName, SystemService> systemServicesMap;

    private WorkerPool workers = null;
    private ErrorReporter errorLog = null;

    private TcpConnectorService netComSvc = null;
    private CommonMessageProcessor msgProc = null;

    private boolean shutdownFinished;
    private ObjectProtection shutdownProt;

    static
    {
        try
        {
            ID_ANON_CLIENT_NAME = new IdentityName("AnonymousClient");
            ROLE_PUBLIC_NAME    = new RoleName("Public");
            TYPE_PUBLIC_NAME    = new SecTypeName("Public");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The " + Controller.class.getName() + " class contains invalid " +
                "security object name constants",
                nameExc
            );
        }
    }

    public Controller(AccessContext sysCtxRef, String[] argsRef)
        throws IOException
    {
        sysCtx = sysCtxRef;
        args = argsRef;

        // Initialize security objects
        {
            AccessContext initCtx = sysCtx.clone();
            try
            {
                initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                anonClientId = new Identity(initCtx, ID_ANON_CLIENT_NAME);
                publicRole = new Role(initCtx, ROLE_PUBLIC_NAME);
                publicType = new SecurityType(initCtx, TYPE_PUBLIC_NAME);
            }
            catch (AccessDeniedException accessExc)
            {
                throw new ImplementationError(
                    "Controller initialization failed: Initialization of security contexts failed",
                    accessExc
                );
            }
        }

        // Create the timer event service
        timerEventSvc = new GenericTimer<>();

        // Create the filesystem event service
        try
        {
            fsEventSvc = new FileSystemWatch();
        }
        catch (IOException ioExc)
        {
            logFailure("Initialization of the FileSystemWatch service failed");
            // FIXME: Generate a startup exception
            throw ioExc;
        }

        systemServicesMap = new TreeMap<>();
        systemServicesMap.put(timerEventSvc.getInstanceName(), timerEventSvc);
        systemServicesMap.put(fsEventSvc.getInstanceName(), fsEventSvc);

        cpuCount = Runtime.getRuntime().availableProcessors();

        shutdownFinished = false;
        shutdownProt = new ObjectProtection(sysCtx);
    }

    @Override
    public void run()
    {
        try
        {
            logInfo("Entering debug console");
            AccessContext debugCtx;
            {
                AccessContext impCtx = sysCtx.clone();
                impCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                debugCtx = impCtx.impersonate(
                    new Identity(impCtx, new IdentityName("LocalDebugConsole")),
                    sysCtx.subjectRole,
                    sysCtx.subjectDomain,
                    sysCtx.getLimitPrivs().toArray()
                );
            }
            debugCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_OBJ_VIEW);
            DebugConsoleImpl dbgConsoleInstance = new DebugConsoleImpl(this, debugCtx);
            dbgConsoleInstance.loadDefaultCommands(System.out, System.err);
            DebugConsole dbgConsole = dbgConsoleInstance;
            dbgConsole.stdStreamsConsole();
            System.out.println();
        }
        catch (Throwable error)
        {
            errorLog.reportError(error);
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
                null
            );
        }
    }

    public void initialize(ErrorReporter errorLogRef)
    {
        shutdownFinished = false;

        errorLog = errorLogRef;

        System.out.printf("\n%s\n\n", SCREEN_DIV);
        programInfo();
        System.out.printf("\n%s\n\n", SCREEN_DIV);

        logInit("Applying base security policy to system objects");
        applyBaseSecurityPolicy();

        logInit("Starting worker thread pool");
        workerThreadCount = cpuCount <= MAX_CPU_COUNT ? cpuCount : MAX_CPU_COUNT;
        {
            int qSize = workerThreadCount * workerQueueFactor;
            workerQueueSize = qSize > MIN_WORKER_QUEUE_SIZE ? qSize : MIN_WORKER_QUEUE_SIZE;
        }
        workers = WorkerPool.initialize(workerThreadCount, workerQueueSize, true, "MainWorkerPool", errorLog);

        // Initialize the message processor
        logInit("Initializing API call dispatcher");
        msgProc = new CommonMessageProcessor(this, workers);

        logInit("Initializing test APIs");
        {
            ApiCall pingCall = new Ping(this, this);
            msgProc.addApiCall(pingCall);
        }

        // Initialize the network communications service
        logInit("Initializing network communications service");
        initializeNetComService();

        // Start service threads
        for (SystemService sysSvc : systemServicesMap.values())
        {
            logInfo(
                String.format(
                    "Starting service instance '%s' of type %s",
                    sysSvc.getInstanceName().displayValue, sysSvc.getServiceName().displayValue
                )
            );
            try
            {
                sysSvc.start();
            }
            catch (SystemServiceStartException startExc)
            {
                errorLog.reportError(startExc);
                logFailure(
                    String.format(
                        "Start of the service instance '%s' of type %s failed",
                        sysSvc.getInstanceName().displayValue, sysSvc.getServiceName().displayValue
                    )
                );
            }
        }


        System.out.printf("\n%s\n\n", SCREEN_DIV);
        runTimeInfo();
        System.out.printf("\n%s\n\n", SCREEN_DIV);
    }

    public void shutdown(AccessContext accCtx) throws AccessDeniedException
    {
        shutdownProt.requireAccess(accCtx, AccessType.USE);
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
            for (SystemService sysSvc : systemServicesMap.values())
            {
                logInfo(
                    String.format(
                        "Shutting down service instance '%s' of type %s",
                        sysSvc.getInstanceName().displayValue, sysSvc.getServiceName().displayValue
                    )
                );
                sysSvc.shutdown();

                logInfo(
                    String.format(
                        "Waiting for service instance '%s' to complete shutdown",
                        sysSvc.getInstanceName().displayValue
                    )
                );
                try
                {
                    sysSvc.awaitShutdown(SHUTDOWN_THR_JOIN_WAIT);
                }
                catch (InterruptedException intrExc)
                {
                    errorLog.reportError(intrExc);
                }
            }

            if (workers != null)
            {
                logInfo("Shutting down worker thread pool");
                workers.shutdown();
                workers = null;
            }

            logInfo("Shutdown complete");
        }
        shutdownFinished = true;
    }

    @Override
    public ErrorReporter getErrorReporter()
    {
        return errorLog;
    }

    @Override
    public Timer<String, Action<String>> getTimer()
    {
        return (Timer) timerEventSvc;
    }

    @Override
    public FileSystemWatch getFsWatch()
    {
        return fsEventSvc;
    }

    public static final void logInit(String what)
    {
        System.out.println("INIT      " + what);
    }

    public static final void logInfo(String what)
    {
        System.out.println("INFO      " + what);
    }

    public static final void logBegin(String what)
    {
        System.out.println("BEGIN     " + what);
    }

    public static final void logEnd(String what)
    {
        System.out.println("END       " + what);
    }

    public static final void logFailure(String what)
    {
        System.err.println("FAILED    " + what);
    }

    public static final void printField(String fieldName, String fieldContent)
    {
        System.out.printf("  %-32s: %s\n", fieldName, fieldContent);
    }

    public final void programInfo()
    {
        System.out.println(
            "Software information\n" +
            "--------------------\n"
        );

        printField("PROGRAM", PROGRAM);
        printField("MODULE", MODULE);
        printField("VERSION", VERSION);
    }

    public final void runTimeInfo()
    {
        Properties sysProps = System.getProperties();
        String jvmSpecVersion = sysProps.getProperty("java.vm.specification.version");
        String jvmVendor = sysProps.getProperty("java.vm.vendor");
        String jvmVersion = sysProps.getProperty("java.vm.version");
        String osName = sysProps.getProperty("os.name");
        String osVersion = sysProps.getProperty("os.version");
        String sysArch = sysProps.getProperty("os.arch");

        System.out.println(
            "Execution environment information\n" +
            "--------------------------------\n"
        );

        Runtime rt = Runtime.getRuntime();
        long freeMem = rt.freeMemory() / 1048576;
        long availMem = rt.maxMemory() / 1048576;

        printField("JAVA PLATFORM", jvmSpecVersion);
        printField("RUNTIME IMPLEMENTATION", jvmVendor + ", Version " + jvmVersion);
        System.out.println();
        printField("SYSTEM ARCHITECTURE", sysArch);
        printField("OPERATING SYSTEM", osName + " " + osVersion);
        printField("AVAILABLE PROCESSORS", Integer.toString(cpuCount));
        if (availMem == Long.MAX_VALUE)
        {
            printField("AVAILABLE MEMORY", "OS ALLOCATION LIMIT");
        }
        else
        {
            printField("AVAILABLE MEMORY", String.format("%10d MiB", availMem));
        }
        printField("FREE MEMORY", String.format("%10d MiB", freeMem));
        System.out.println();
        printField("WORKER THREADS", Integer.toString(workers.getThreadCount()));
        printField("WORKER QUEUE SIZE", Integer.toString(workers.getQueueSize()));
        printField("WORKER SCHEDULING", workers.isFairQueue() ? "FIFO" : "Random");
    }

    private void applyBaseSecurityPolicy()
    {
        PrivilegeSet effPriv = sysCtx.getEffectivePrivs();
        try
        {
            // Enable all privileges
            effPriv.enablePrivileges(Privilege.PRIV_SYS_ALL);

            // Allow CONTROL access by domain SYSTEM to type SYSTEM
            SecurityType sysType = sysCtx.getDomain();
            sysType.addEntry(sysCtx, sysType, AccessType.CONTROL);

            // Allow USE access by role SYSTEM to shutdownProt
            shutdownProt.addAclEntry(sysCtx, sysCtx.getRole(), AccessType.USE);
        }
        catch (AccessDeniedException accExc)
        {
            logFailure("Applying the base security policy failed");
            errorLog.reportError(accExc);
        }
        finally
        {
            effPriv.disablePrivileges(Privilege.PRIV_SYS_ALL);
        }
    }

    public static void main(String[] args)
    {
        logInit("System components initialization in progress");

        logInit("Constructing error reporter instance");
        ErrorReporter errorLog = new DebugErrorReporter();

        try
        {
            logInit("Initializing system security context");
            Initializer sysInit = new Initializer();
            logInit("Constructing controller instance");
            Controller instance = sysInit.initController(args);

            logInit("Initializing controller services");
            instance.initialize(errorLog);

            logInit("Initialization complete");
            System.out.println();

            Thread.currentThread().setName("MainLoop");

            logInfo("Starting controller module");
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

    private void initializeNetComService()
    {
        try
        {
            AccessContext defaultPeerAccCtx;
            {
                AccessContext impCtx = sysCtx.clone();
                impCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                defaultPeerAccCtx = impCtx.impersonate(
                    anonClientId,
                    publicRole,
                    publicType
                );
            }
            netComSvc = new TcpConnectorService(
                this,
                msgProc,
                defaultPeerAccCtx
            );
            netComSvc.initialize();
            systemServicesMap.put(netComSvc.getInstanceName(), netComSvc);
        }
        catch (AccessDeniedException accessExc)
        {
            errorLog.reportError(accessExc);
            logFailure("Cannot create security context for the network communications service");
        }
        catch (IOException | SystemServiceStartException exc)
        {
            errorLog.reportError(exc);
        }
    }

    private class TcpConnTracker implements TcpConnectionObserver
    {
        private Controller controller;

        TcpConnTracker(Controller controllerRef)
        {
            controller = controllerRef;
        }

        @Override
        public void outboundConnectionEstablished(Peer connPeer)
        {
            // TODO: Satellite or utility connection connected
        }

        @Override
        public void inboundConnectionEstablished(Peer connPeer)
        {
            CtlPeerContext peerCtx = new CtlPeerContext();
            connPeer.attach(peerCtx);
        }

        @Override
        public void connectionClosed(Peer connPeer)
        {
            // TODO: Implement
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    public interface DebugConsole
    {
        Map<String, ControllerDebugCmd> getCommandMap();
        void stdStreamsConsole();
        void streamsConsole(
            InputStream debugIn,
            PrintStream debugOut,
            PrintStream debugErr,
            boolean prompt
        );
        void processCommandLine(
            PrintStream debugOut,
            PrintStream debugErr,
            String commandLine
        );
        void processCommand(
            PrintStream debugOut,
            PrintStream debugErr,
            String commandUpperCase,
            Map<String, String> parameters
        );
        void loadCommand(
            PrintStream debugOut,
            PrintStream debugErr,
            String cmdClassName
        );
        void unloadCommand(
            PrintStream debugOut,
            PrintStream debugErr,
            String cmdClassName
        );
        void exitConsole();
    }

    private static class DebugConsoleImpl implements DebugConsole
    {
        private enum ParamParserState
        {
            SKIP_COMMAND,
            SPACE,
            OPTIONAL_SPACE,
            READ_KEY,
            READ_VALUE,
            ESCAPE
        };

        private final Controller controller;
        private final AccessContext debugCtx;

        public static final String CONSOLE_PROMPT = "Command ==> ";

        private Map<String, String> parameters;

        private boolean loadedCmds  = false;
        private boolean exitFlag    = false;

        private Map<String, ControllerDebugCmd> commandMap;
        public static final String[] COMMAND_CLASS_LIST =
        {
            "CmdDisplayThreads",
            "CmdDisplayContextInfo",
            "CmdDisplayServices",
            "CmdStartService",
            "CmdEndService",
            "CmdTestErrorLog",
            "CmdShutdown"
        };
        public static final String COMMAND_CLASS_PKG = "com.linbit.drbdmanage.debug";

        private DebugControl debugCtl;

        DebugConsoleImpl(Controller controllerRef, AccessContext accCtx)
        {
            ErrorCheck.ctorNotNull(DebugConsoleImpl.class, Controller.class, controllerRef);
            ErrorCheck.ctorNotNull(DebugConsoleImpl.class, AccessContext.class, accCtx);
            controller = controllerRef;
            debugCtx = accCtx;
            parameters = new TreeMap<>();
            commandMap = new TreeMap<>();
            loadedCmds = false;
            debugCtl = new DebugControlImpl(controller);
        }

        @Override
        public Map<String, ControllerDebugCmd> getCommandMap()
        {
            return commandMap;
        }

        public void loadDefaultCommands(
            PrintStream debugOut,
            PrintStream debugErr
        )
        {
            if (!loadedCmds)
            {
                for (String cmdClassName : COMMAND_CLASS_LIST)
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
                    COMMAND_CLASS_PKG + "." + cmdClassName
                );
                try
                {
                    ControllerDebugCmd debugCmd = (ControllerDebugCmd) cmdClass.newInstance();
                    debugCmd.initialize(controller, controller, debugCtl, this);

                    // FIXME: Detect and report name collisions
                    for (String cmdName : debugCmd.getCmdNames())
                    {
                        commandMap.put(cmdName.toUpperCase(), debugCmd);
                    }
                }
                catch (IllegalAccessException | InstantiationException instantiateExc)
                {
                    controller.errorLog.reportError(instantiateExc);
                }
            }
            catch (ClassNotFoundException cnfExc)
            {
                controller.errorLog.reportError(cnfExc);
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

        @Override
        public void stdStreamsConsole()
        {
            streamsConsole(System.in, System.out, System.err, true);
        }

        @Override
        public void streamsConsole(
            InputStream debugIn,
            PrintStream debugOut,
            PrintStream debugErr,
            boolean prompt
        )
        {
            try
            {
                BufferedReader cmdIn = new BufferedReader(
                    new InputStreamReader(debugIn)
                );

                String inputLine;
                commandLineLoop:
                while (!exitFlag)
                {
                    // Print the console prompt if required
                    if (prompt)
                    {
                        debugOut.print("\n");
                        debugOut.print(CONSOLE_PROMPT);
                        debugOut.flush();
                    }

                    inputLine = cmdIn.readLine();
                    if (inputLine != null)
                    {
                        processCommandLine(debugOut, debugErr, inputLine);
                    }
                    else
                    {
                        // End of command input stream, exit console
                        break;
                    }
                }
            }
            catch (IOException ioExc)
            {
                controller.errorLog.reportError(ioExc);
            }
            catch (Throwable error)
            {
                controller.errorLog.reportError(error);
            }
        }

        @Override
        public void processCommandLine(
            PrintStream debugOut,
            PrintStream debugErr,
            String inputLine
        )
        {
            String commandLine = inputLine.trim();
            if (!inputLine.isEmpty())
            {
                if (inputLine.startsWith("?"))
                {
                    findCommands(debugOut, debugErr, inputLine.substring(1, inputLine.length()).trim());
                }
                else
                {
                    // Parse the debug command line
                    char[] commandChars = commandLine.toCharArray();
                    String command = parseCommandName(commandChars);

                    try
                    {
                        parseCommandParameters(commandChars, parameters);
                        processCommand(debugOut, debugErr, command, parameters);
                    }
                    catch (ParseException parseExc)
                    {
                        debugErr.println(parseExc.getMessage());
                    }
                    finally
                    {
                        parameters.clear();
                    }
                }
            }
        }

        @Override
        public void processCommand(
            PrintStream debugOut,
            PrintStream debugErr,
            String command,
            Map<String, String> parameters
        )
        {
            String cmdName = command.toUpperCase();
            ControllerDebugCmd debugCmd = commandMap.get(cmdName);
            if (debugCmd != null)
            {
                String displayCmdName = debugCmd.getDisplayName(cmdName);
                if (displayCmdName == null)
                {
                    displayCmdName = "<unknown command>";
                }

                String cmdInfo = debugCmd.getCmdInfo();
                if (cmdInfo == null)
                {
                    System.out.printf("\u001b[1;37m%-20s\u001b[0m\n", displayCmdName);
                }
                else
                {
                    System.out.printf("\u001b[1;37m%-20s\u001b[0m %s\n", displayCmdName, cmdInfo);
                }
                try
                {
                    debugCmd.execute(debugOut, debugErr, debugCtx, parameters);
                }
                catch (Exception exc)
                {
                    controller.errorLog.reportError(exc);
                }
            }
            else
            {
                debugErr.printf(
                    "Error:\n" +
                    "    The statement '%s' is not a valid debug console command.\n" +
                    "Correction:\n" +
                    "    Enter a valid debug console command.\n" +
                    "    Use the \"?\" builtin query (without quotes) to display a list\n" +
                    "    of available commands.\n",
                    command
                );
            }
        }

        public void findCommands(PrintStream debugOut, PrintStream debugErr, String cmdPattern)
        {
            try
            {
                debugOut.printf("\u001b[1;37m%-20s\u001b[0m %s\n", "?", "Find commands");
                Map<String, ControllerDebugCmd> selectedCmds;

                if (cmdPattern.isEmpty())
                {
                    // Select all commands
                    selectedCmds = commandMap;
                }
                else
                {
                    // Filter commands using the regular expression pattern
                    selectedCmds = new TreeMap<>();
                    Pattern cmdRegEx = Pattern.compile(cmdPattern, Pattern.CASE_INSENSITIVE);
                    for (Map.Entry<String, ControllerDebugCmd> cmdEntry : commandMap.entrySet())
                    {
                        String cmdName = cmdEntry.getKey();
                        Matcher cmdMatcher = cmdRegEx.matcher(cmdName);
                        if (cmdMatcher.matches())
                        {
                            selectedCmds.put(cmdEntry.getKey(), cmdEntry.getValue());
                        }
                    }
                }

                // Generate the output list
                StringBuilder cmdNameList = new StringBuilder();
                for (Map.Entry<String, ControllerDebugCmd> cmdEntry : selectedCmds.entrySet())
                {
                    String cmdName = cmdEntry.getKey();
                    ControllerDebugCmd debugCmd = cmdEntry.getValue();

                    String cmdInfo = debugCmd.getCmdInfo();
                    if (cmdInfo == null)
                    {
                        cmdNameList.append(
                            String.format("    %-20s\n", cmdName)
                        );
                    }
                    else
                    {
                        cmdNameList.append(
                            String.format("    %-20s %s\n", cmdName, cmdInfo)
                        );
                    }
                }

                // Write results
                if (cmdNameList.length() > 0)
                {
                    debugOut.println("Matching commands:");
                    debugOut.print(cmdNameList.toString());
                    debugOut.flush();
                }
                else
                {
                    if (cmdPattern.isEmpty())
                    {
                        debugOut.println("No commands are available.");
                    }
                    else
                    {
                        debugOut.println("No commands matching the pattern were found.");
                    }
                }
            }
            catch (PatternSyntaxException patternExc)
            {
                debugErr.println(
                    "Error:\n" +
                    "    The specified regular expression pattern is not valid.\n" +
                    "    (See error details section for a more detailed description of the error)\n" +
                    "Correction:\n" +
                    "    If any text is entered following the ? sign, the text must form a valid\n" +
                    "    regular expression pattern.\n" +
                    "Error details:\n"
                );
                debugErr.println(patternExc.getMessage());
            }
        }

        public void exitConsole()
        {
            exitFlag = true;
        }

        private String parseCommandName(char[] commandChars)
        {
            int commandLength = commandChars.length;
            for (int idx = 0; idx < commandChars.length; ++idx)
            {
                if (commandChars[idx] == ' ' || commandChars[idx] == '\t')
                {
                    commandLength = idx;
                    break;
                }
            }
            String command = new String(commandChars, 0, commandLength);
            return command;
        }

        private void parseCommandParameters(char[] commandChars, Map<String, String> parameters)
            throws ParseException
        {
            int keyOffset = 0;
            int valueOffset = 0;
            int length = 0;

            ParamParserState state = ParamParserState.SKIP_COMMAND;
            String key = null;
            String value = null;
            for (int idx = 0; idx < commandChars.length; ++idx)
            {
                char cc = commandChars[idx];
                switch (state)
                {
                    case SKIP_COMMAND:
                        if (cc == ' ' || cc == '\t')
                        {
                            state = ParamParserState.OPTIONAL_SPACE;
                        }
                        break;
                    case SPACE:
                        if (cc != ' ' && cc != '\t')
                        {
                            errorParser(idx);
                        }
                        state = ParamParserState.OPTIONAL_SPACE;
                        break;
                    case OPTIONAL_SPACE:
                        if (cc == ' ' || cc == '\t')
                        {
                            break;
                        }
                        keyOffset = idx;
                        state = ParamParserState.READ_KEY;
                        // fall-through
                    case READ_KEY:
                        if (cc == '(')
                        {
                            length = idx - keyOffset;
                            if (length < 1)
                            {
                                errorInvalidKey(keyOffset);
                            }
                            key = new String(commandChars, keyOffset, length).toUpperCase();
                            valueOffset = idx + 1;
                            state = ParamParserState.READ_VALUE;
                        }
                        else
                        if (!((cc >= 'a' && cc <= 'z') || (cc >= 'A' && cc <= 'Z')))
                        {
                            if (!(cc >= '0' && cc <= '9' && idx > keyOffset))
                            {
                                errorInvalidKey(keyOffset);
                            }
                        }
                        break;
                    case READ_VALUE:
                        if (cc == ')')
                        {
                            length = idx - valueOffset;
                            value = new String(commandChars, valueOffset, length);
                            if (key != null)
                            {
                                parameters.put(key, value);
                            }
                            else
                            {
                                errorInvalidKey(keyOffset);
                            }
                            state = ParamParserState.OPTIONAL_SPACE;
                        }
                        else
                        if (cc == '\\')
                        {
                            // Ignore the next character's special meaning
                            state = ParamParserState.ESCAPE;
                        }
                        break;
                    case ESCAPE:
                        // Only values can be escaped, so the next state is always READ_VALUE
                        state = ParamParserState.READ_VALUE;
                        break;
                    default:
                        throw new ImplementationError(
                            String.format(
                                "Missing case label for enum member '%s'",
                                state.name()
                            ),
                            null
                        );
                }
            }
            if (state != ParamParserState.SKIP_COMMAND && state != ParamParserState.OPTIONAL_SPACE)
            {
                errorIncompleteLine(commandChars.length);
            }
        }

        private void errorInvalidKey(int pos) throws ParseException
        {
            throw new ParseException(
                String.format(
                    "The command line is not valid. " +
                    "An invalid parameter key was encountered at position %d.",
                    pos
                ),
                pos
            );
        }

        private void errorParser(int pos) throws ParseException
        {
            throw new ParseException(
                String.format(
                    "The command line is not valid. " +
                    "The parser encountered an error at position %d.",
                    pos
                ),
                pos
            );
        }

        private void errorIncompleteLine(int pos) throws ParseException
        {
            throw new ParseException(
                "The command line is not valid. The input line appears to have been truncated.",
                pos
            );
        }
    }

    public interface DebugControl
    {
        Map<ServiceName, SystemService> getSystemServiceMap();
        void shutdown(AccessContext accCtx);
    }

    private static class DebugControlImpl implements DebugControl
    {
        Controller controller;

        DebugControlImpl(Controller controllerRef)
        {
            controller = controllerRef;
        }

        public Map<ServiceName, SystemService> getSystemServiceMap()
        {
            Map<ServiceName, SystemService> svcCpy = new TreeMap<>();
            svcCpy.putAll(controller.systemServicesMap);
            return svcCpy;
        }

        public void shutdown(AccessContext accCtx)
        {
            try
            {
                controller.shutdown(accCtx);
            }
            catch (AccessDeniedException accExc)
            {
                controller.errorLog.reportError(accExc);
            }
        }
    }
}
