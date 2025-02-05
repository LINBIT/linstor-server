package com.linbit.linstor.core;

import com.linbit.Platform;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.InjectorLoader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.google.inject.Module;

/**
 * LinStor common constants and basic utilities
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class LinStor
{
    public static final String CONTROLLER_PROPS = "ControllerProps";
    public static final String SATELLITE_PROPS = "SatelliteProps";

    public static final String FIPS_CRYPTO_MODULE_NAME  = "com.linbit.linstor.modularcrypto.FipsCryptoModule";
    public static final String JCL_CRYPTO_MODULE_NAME   = "com.linbit.linstor.modularcrypto.JclCryptoModule";
    public static final String FIPS_INIT_MODULE_NAME    = "com.linbit.linstor.modularcrypto.FipsInit";

    // Path to the DRBD configuration files; this should be replaced by some meaningful constant or possibly
    // a value configurable in the cluster configuration
    public static final String CONFIG_PATH = "/var/lib/linstor.d";
    public static final String BACKUP_PATH = CONFIG_PATH + "/.backup";

    private static final int MEGA_BYTE = 1048576;

    public static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    // ============================================================
    // Product and version information
    //
    public static final String SOFTWARE_CREATOR = "LINBIT\u00AE";

    public static final String PROGRAM = "LINSTOR";
    public static final VersionInfoProvider VERSION_INFO_PROVIDER = new VersionInfoProviderImpl();

    public static final String DISKLESS_STOR_POOL_NAME = "DfltDisklessStorPool";

    public static final String SKIP_HOSTNAME_CHECK_KEY = "SkipHostnameCheck";

    public static final String CONTROLLER_MODULE = "Controller";

    public static final String SATELLITE_MODULE = "Satellite";

    public static final String KEY_NODE_NAME = "NodeName";
    public static final String PROP_KEY_CLUSTER_ID = "Cluster/LocalID";

    public static final double OVERSUBSCRIPTION_RATIO_DEFAULT = 20.0;
    public static final double OVERSUBSCRIPTION_RATIO_UNKOWN = -1.0;

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
        System.out.println(linstorInfo());
        System.out.println();

        System.out.println("System components initialization in progress\n");
    }

    public static String linstorInfo()
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

        return String.format(
            "Version:            %s (%s)\n" +
            "Build time:         %s Log v2\n" +
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
    }

    /**
     * Returns the system hostname.
     * On error returns an empty string.
     *
     * @return Hostname by 'uname -n'.
     */
    public static String getHostName()
    {
        String hostname = "";

        if (Platform.isLinux())
        {
            hostname = getUname("-n");
        }
        else if (Platform.isWindows())
        {
            hostname = System.getenv().get("COMPUTERNAME");
            try
            {
                Process process = new ProcessBuilder("hostname").start();
                process.waitFor(1, TimeUnit.SECONDS);
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                hostname = br.readLine().trim();
            }
            catch (IOException | InterruptedException ignored)
            {
            }
        }
        return hostname;
    }

    public static String getUname(String param)
    {
        String uname = "";
        try
        {
            Process process = new ProcessBuilder("uname", param).start();
            process.waitFor(1, TimeUnit.SECONDS);
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            uname = br.readLine().trim();
        }
        catch (IOException | InterruptedException ignored)
        {
        }

        return uname;
    }

    static void loadModularCrypto(
        final List<Module> injModList,
        final ErrorReporter errorLog,
        final boolean haveFipsInit
    )
    {
        final boolean haveFipsCrypto = InjectorLoader.dynLoadInjModule(
            FIPS_CRYPTO_MODULE_NAME,
            injModList,
            errorLog,
            null
        );
        final boolean haveJclCrypto = InjectorLoader.dynLoadInjModule(
            JCL_CRYPTO_MODULE_NAME,
            injModList, errorLog, null
        );
        if (haveFipsCrypto && haveJclCrypto)
        {
            throw new Error(
                "Faulty installation: Mutually exclusive FIPS 140 and JCL cryptography providers are both installed"
            );
        }
        if (!haveFipsCrypto && !haveJclCrypto)
        {
            throw new Error(
                "Faulty installation: No cryptography provider could be loaded"
            );
        }

        // If the FIPS crypto module is loaded but FIPS initialization didn't run,
        // or the FIPS crypto module is NOT loaded, but FIPS initialization did run,
        // something isn't right with the cryptography installation
        if (haveFipsCrypto ^ haveFipsInit)
        {
            throw new Error(
                "Faulty installation: Incomplete FIPS 140 initialization"
            );
        }
    }

    static boolean initializeFips(final ErrorReporter errorLog)
        throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        boolean haveFipsInit = false;
        try
        {
            Class<?> fipsInitCls = Class.forName(FIPS_INIT_MODULE_NAME);
            if (fipsInitCls != null)
            {
                final Method fipsInitMethod = fipsInitCls.getMethod("initialize", ErrorReporter.class);
                fipsInitMethod.invoke(null, errorLog);
                haveFipsInit = true;
            }
        }
        catch (ClassNotFoundException ignored)
        {
        }
        return haveFipsInit;
    }

    public static final Path SOS_REPORTS_DIR = Paths.get(CONFIG_PATH + "/sos-reports/");

    private LinStor()
    {
    }
}
