package com.linbit.linstor.core;

import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.timer.CoreTimer;

import java.io.PrintStream;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * LinStor common base class for the Controller and Satellite modules
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class LinStor
{
    private static final int MEGA_BYTE = 1048576;

    public static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    // ============================================================
    // Product and version information
    //
    public static final String SOFTWARE_CREATOR = "LINBIT\u00AE";

    public static final String PROGRAM = "LINSTOR";
    public static final VersionInfoProvider VERSION_INFO_PROVIDER = new VersionInfoProviderImpl();

    public static final String DISKLESS_STOR_POOL_NAME = "DfltDisklessStorPool";

    // ============================================================
    // Core system services
    //
    protected CoreTimer timerEventSvc;

    // Error & exception logging facility
    protected ErrorReporter errorReporter;

    // Synchronization lock for major global changes
    public ReadWriteLock reconfigurationLock;

    public static void printRunTimeInfo(PrintStream out)
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

        printField(out, "JAVA PLATFORM:", jvmSpecVersion);
        printField(out, "RUNTIME IMPLEMENTATION:", jvmVendor + ", Version " + jvmVersion);
        out.println();
        printField(out, "SYSTEM ARCHITECTURE:", sysArch);
        printField(out, "OPERATING SYSTEM:", osName + " " + osVersion);
        printField(out, "AVAILABLE PROCESSORS:", Integer.toString(CPU_COUNT));

        long freeMem = rt.freeMemory() / MEGA_BYTE;
        long availMem = rt.maxMemory() / MEGA_BYTE;
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

    public static void printField(PrintStream out, String title, String text)
    {
        out.printf("    %-24s %s\n", title, text);
    }

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
        long availMem = rt.maxMemory() / MEGA_BYTE;

        System.out.printf(
            "Version:            %s (%s)\n" +
            "Build time:         %s\n" +
            "Java Version:       %s\n" +
            "Java VM:            %s, Version %s\n" +
            "Operating system:   %s, Version %s\n" +
            "Environment:        %s, %d processors, %d MiB memory reserved for allocations\n",
            VERSION_INFO_PROVIDER.getVersion(), VERSION_INFO_PROVIDER.getGitCommitId(),
            VERSION_INFO_PROVIDER.getBuildTime(),
            jvmSpecVersion,
            jvmVendor, jvmVersion,
            osName, osVersion,
            sysArch, cpus, availMem
        );
        System.out.println();

        System.out.println("System components initialization in progress\n");
    }
}
