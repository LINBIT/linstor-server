package com.linbit.drbdmanage;

import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.Privilege;
import com.linbit.drbdmanage.timer.CoreTimer;
import com.linbit.drbdmanage.timer.CoreTimerImpl;
import java.io.PrintStream;
import java.util.Properties;

/**
 * DrbdManage common base class for the Controller and Satellite modules
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class DrbdManage
{
    // ============================================================
    // Product and version information
    //
    public static final String PROGRAM = "drbdmanageNG";
    public static final String VERSION = "experimental 2017-04-19_001";

    // ============================================================
    // Worker thread pool defaults
    //
    public static final int MIN_WORKER_QUEUE_SIZE = 32;
    public static final int MAX_CPU_COUNT = 1024;

    // At shutdown, wait at most SHUTDOWN_THR_JOIN_WAIT milliseconds for
    // a service thread to end
    public static final long SHUTDOWN_THR_JOIN_WAIT = 3000L;

    // Queue slots per worker thread
    private int workerQueueFactor = 4;
    private int workerQueueSize = MIN_WORKER_QUEUE_SIZE;

    // Default configuration
    private int cpuCount = 8;
    private int workerThreadCount = 8;

    // ============================================================
    // Core system services
    //
    private CoreTimer timerEventSvc;

    // Error & exception logging facility
    private ErrorReporter errorLog;

    DrbdManage()
    {
        // Initialize maps

        // Initialize system services
        timerEventSvc = new CoreTimerImpl();

        // Initialize system variables
        cpuCount = Runtime.getRuntime().availableProcessors();

        // Null-initialize remaining components
        errorLog = null;
    }

    /**
     * Destroys the debug console instance of a connected peer
     *
     * @param accCtx The access context to authorize this API call
     * @param client Connected peer
     * @throws AccessDeniedException If the API call is not authorized
     */
    public void destroyDebugConsole(AccessContext accCtx, Peer client)
        throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        CommonPeerCtx peerContext = (CommonPeerCtx) client.getAttachment();
        peerContext.setDebugConsole(null);
    }

    public void startSystemServices(Iterable<SystemService> services, ErrorReporter errLog)
    {
        for (SystemService sysSvc : services)
        {
            logInfo(
                String.format(
                    "Starting service instance '%s' of type %s",
                    sysSvc.getInstanceName().displayValue, sysSvc.getServiceName().displayValue
                )
            );
            boolean successFlag = false;
            try
            {
                sysSvc.start();
                successFlag = true;
            }
            catch (SystemServiceStartException startExc)
            {
                errLog.reportError(startExc);

            }
            catch (Exception unhandledExc)
            {
                errLog.reportError(unhandledExc);
            }
            finally
            {
                if (!successFlag)
                {
                    logFailure(
                        String.format(
                            "Start of the service instance '%s' of type %s failed",
                            sysSvc.getInstanceName().displayValue, sysSvc.getServiceName().displayValue
                        )
                    );
                }
            }
        }
    }

    public void stopSystemServices(Iterable<SystemService> services, ErrorReporter errLog)
    {
        // Shutdown service threads
        for (SystemService sysSvc : services)
        {
            logInfo(
                String.format(
                    "Shutting down service instance '%s' of type %s",
                    sysSvc.getInstanceName().displayValue, sysSvc.getServiceName().displayValue
                )
            );
            boolean successFlag = true;
            try
            {
                sysSvc.shutdown();

                logInfo(
                    String.format(
                        "Waiting for service instance '%s' to complete shutdown",
                        sysSvc.getInstanceName().displayValue
                    )
                );
                sysSvc.awaitShutdown(SHUTDOWN_THR_JOIN_WAIT);
            }
            catch (Exception unhandledExc)
            {
                errorLog.reportError(unhandledExc);
            }
            finally
            {
                if (!successFlag)
                {
                    logFailure(
                        String.format(
                            "Shutdown of the service instance '%s' of type %s failed",
                            sysSvc.getInstanceName().displayValue, sysSvc.getServiceName().displayValue
                        )
                    );
                }
            }
        }
    }

    public CoreTimer getTimer()
    {
        return timerEventSvc;
    }

    public ErrorReporter getErrorReporter()
    {
        return errorLog;
    }

    public void setErrorLog(AccessContext accCtx, ErrorReporter errorLogRef)
        throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        errorLog = errorLogRef;
    }

    public int getWorkerQueueSize()
    {
        return workerQueueSize;
    }

    public void setWorkerQueueSize(AccessContext accCtx, int size) throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        workerQueueSize = size;
    }

    public int getWorkerThreadCount()
    {
        return workerThreadCount;
    }

    public void setWorkerThreadCount(AccessContext accCtx, int count) throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        workerThreadCount = count;
    }

    public int getWorkerQueueFactor()
    {
        return workerQueueFactor;
    }

    public void setWorkerQueueFactor(AccessContext accCtx, int factor) throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        workerQueueFactor = factor;
    }

    public int getCpuCount()
    {
        return cpuCount;
    }

    public void printRunTimeInfo(PrintStream out)
    {
        Properties sysProps = System.getProperties();
        String jvmSpecVersion = sysProps.getProperty("java.vm.specification.version");
        String jvmVendor = sysProps.getProperty("java.vm.vendor");
        String jvmVersion = sysProps.getProperty("java.vm.version");
        String osName = sysProps.getProperty("os.name");
        String osVersion = sysProps.getProperty("os.version");
        String sysArch = sysProps.getProperty("os.arch");

        out.println("Execution environment information\n");

        Runtime rt = Runtime.getRuntime();
        long freeMem = rt.freeMemory() / 1048576;
        long availMem = rt.maxMemory() / 1048576;

        printField(out, "JAVA PLATFORM:", jvmSpecVersion);
        printField(out, "RUNTIME IMPLEMENTATION:", jvmVendor + ", Version " + jvmVersion);
        out.println();
        printField(out, "SYSTEM ARCHITECTURE:", sysArch);
        printField(out, "OPERATING SYSTEM:", osName + " " + osVersion);
        printField(out, "AVAILABLE PROCESSORS:", Integer.toString(getCpuCount()));
        if (availMem == Long.MAX_VALUE)
        {
            printField(out, "AVAILABLE MEMORY:", "OS ALLOCATION LIMIT");
        }
        else
        {
            printField(out, "AVAILABLE MEMORY:", String.format("%10d MiB", availMem));
        }
        printField(out, "FREE MEMORY:", String.format("%10d MiB", freeMem));
    }

    public void printField(PrintStream out, String title, String text)
    {
        System.out.printf("    %-24s %s\n", title, text);
    }

    public abstract void logInit(String message);
    public abstract void logInfo(String message);
    public abstract void logWarning(String message);
    public abstract void logError(String message);
    public abstract void logFailure(String message);
    public abstract void logDebug(String message);

    public static void printStartupInfo()
    {
        String unknown = "unknown";
        Properties sysProps = System.getProperties();
        String jvmSpecVersion = sysProps.getProperty("java.vm.specification.version", unknown);
        String jvmVendor = sysProps.getProperty("java.vm.vendor", unknown);
        String jvmVersion = sysProps.getProperty("java.vm.version", unknown);
        String osName = sysProps.getProperty("os.name", unknown);
        String osVersion = sysProps.getProperty("os.version", unknown);
        String sysArch = sysProps.getProperty("os.arch", unknown);
        Runtime rt = Runtime.getRuntime();
        int cpus = rt.availableProcessors();
        long availMem = rt.maxMemory() / 1048576;

        System.out.printf(
            "Java Version:       %s\n" +
            "Java VM:            %s, Version %s\n" +
            "Operating system:   %s, Version %s\n" +
            "Environment:        %s, %d processors, %d MiB memory reserved for allocations\n",
            jvmSpecVersion,
            jvmVendor, jvmVersion,
            osName, osVersion,
            sysArch, cpus, availMem
        );
        System.out.println();

        System.out.println("System components initialization in progress\n");
    }
}
