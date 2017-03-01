package com.linbit.drbdmanage;

import com.linbit.ImplementationError;
import com.linbit.WorkerPool;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.Initializer;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.timer.Action;
import com.linbit.timer.GenericTimer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * drbdmanageNG controller prototype
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class Controller implements Runnable
{
    public static final String PROGRAM = "drbdmanageNG";
    public static final String MODULE = "Controller";
    public static final String VERSION = "experimental 2017-02-23_001";

    public static final int MIN_WORKER_QUEUE_SIZE = 32;
    public static final int MAX_CPU_COUNT = 1024;

    // Defaults
    private int cpuCount = 8;
    private int workerThreadCount = 8;
    // Queue slots per worker thread
    private int workerQueueFactor = 4;
    private int workerQueueSize = MIN_WORKER_QUEUE_SIZE;

    public static final String SCREEN_DIV =
        "------------------------------------------------------------------------------";

    private final AccessContext sysCtx;
    private String[] args;

    private final GenericTimer<String, Action<String>> timerEventSvc;
    private final FileSystemWatch fsEventSvc;

    private WorkerPool workers = null;
    private ErrorReporter errorLog = null;

    public Controller(AccessContext sysCtxRef, String[] argsRef)
        throws IOException
    {
        sysCtx = sysCtxRef;
        args = argsRef;

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

        cpuCount = Runtime.getRuntime().availableProcessors();
    }

    @Override
    public void run()
    {
        try
        {
            logInfo("Entering debug console");
            debugConsole();
        }
        catch (Throwable error)
        {
            errorLog.reportError(error);
        }

        shutdown();
    }

    public void debugConsole()
    {
        try
        {
            BufferedReader stdin = new BufferedReader(
                new InputStreamReader(System.in)
            );

            String commandLine;
            commandLoop:
            do
            {
                System.out.print("\nCommand ==> ");
                System.out.flush();
                commandLine = stdin.readLine();
                if (commandLine != null)
                {
                    StringTokenizer cmdTokens = new StringTokenizer(commandLine);
                    try
                    {
                        String command = cmdTokens.nextToken().toUpperCase();
                        command = command.trim();

                        switch (command)
                        {
                            case "SHUTDOWN":
                                break commandLoop;
                            case "DSPTHR":
                                // fall-through
                            case "THREADS":
                                cmdThreads();
                                break;
                            case "TSTERRLOG":
                                // fall-through
                            case "TESTERRORLOG":
                                throw new TestException(
                                    "Thrown by TSTERRLOG debug command for test purposes",
                                    new IllegalArgumentException(
                                        "Thrown by TSTERRLOG debug command for test purposes"
                                    )
                                );
                            default:
                                if (!command.isEmpty())
                                {
                                    System.err.printf(
                                        "The statement '%s' is not a valid command\n",
                                        command
                                    );
                                }
                                break;
                        }
                    }
                    catch (NoSuchElementException missingToken)
                    {
                        logFailure("A required argument was missing in the command line");
                    }
                    catch (TestException testExc)
                    {
                        errorLog.reportError(testExc);
                    }
                }
            }
            while (commandLine != null);
        }
        catch (IOException ioExc)
        {
            errorLog.reportError(ioExc);
        }
        catch (Throwable error)
        {
            errorLog.reportError(error);
        }
    }

    public void cmdThreads()
    {
        int thrCount = Thread.activeCount();
        Thread[] activeThreads = new Thread[thrCount + 20];
        thrCount = Thread.enumerate(activeThreads);

        System.out.printf(
            "%-32s %18s %4s %-6s %-6s %-6s\n",
            "Thread name", "Id", "Prio", "Alive", "Daemon", "Intr"
        );
        System.out.printf("%s\n", SCREEN_DIV);

        int slot = 0;
        for (Thread thr : activeThreads)
        {
            System.out.printf(
                "%-32s %18d %4d %-6s %-6s %-6s\n",
                thr.getName(),
                thr.getId(),
                thr.getPriority(),
                thr.isAlive() ? "Y" : "N",
                thr.isDaemon() ? "Y" : "N",
                thr.isInterrupted() ? "Y" : "N"
            );

            ++slot;
            if (slot >= thrCount)
            {
                break;
            }
        }
        System.out.printf("%s\n", SCREEN_DIV);
    }

    public void initialize(ErrorReporter errorLogRef)
    {
        errorLog = errorLogRef;

        System.out.printf("\n%s\n\n", SCREEN_DIV);
        programInfo();
        System.out.printf("\n%s\n\n", SCREEN_DIV);

        logInit("Starting timer event service");
        // Start the timer event service
        timerEventSvc.setTimerName("TimerEventService");
        timerEventSvc.start();

        logInit("Starting filesystem event service");
        // Start the filesystem event service
        fsEventSvc.start();

        logInit("Starting worker thread pool");
        workerThreadCount = cpuCount <= MAX_CPU_COUNT ? cpuCount : MAX_CPU_COUNT;
        {
            int qSize = workerThreadCount * workerQueueFactor;
            workerQueueSize = qSize > MIN_WORKER_QUEUE_SIZE ? qSize : MIN_WORKER_QUEUE_SIZE;
        }
        workers = WorkerPool.initialize(workerThreadCount, workerQueueSize, true, "MainWorkerPool");

        System.out.printf("\n%s\n\n", SCREEN_DIV);
        runTimeInfo();
        System.out.printf("\n%s\n\n", SCREEN_DIV);
    }

    public void shutdown()
    {
        logInfo("Shutdown in progress");
        logInfo("Shutting down filesystem event service");
        // Stop the filesystem event service
        fsEventSvc.shutdown();

        logInfo("Shutting down timer event service");
        timerEventSvc.shutdown();

        logInfo("Shutting down worker thread pool");
        workers.shutdown();

        logInfo("Shutdown complete");
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
            "Execution enviroment information\n" +
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

    public static void main(String[] args)
    {
        logInit("System components initialization in progress");

        logInit("Constructing error reporter instance");
        ErrorReporter errorLog = new ErrorReporter();

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

    static class TestException extends Exception
    {
        TestException()
        {
        }

        TestException(String message)
        {
            super(message);
        }

        TestException(Throwable cause)
        {
            super(cause);
        }

        TestException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }
}
