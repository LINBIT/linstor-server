package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.CommonPeerCtx;
import com.linbit.linstor.CoreServices;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.BaseApiCall;
import com.linbit.linstor.dbdrivers.DatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.CommonMessageProcessor;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.DbAccessor;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SecurityLevel;
import com.linbit.linstor.timer.CoreTimer;
import com.linbit.linstor.timer.CoreTimerImpl;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.slf4j.event.Level;

/**
 * LinStor common base class for the Controller and Satellite modules
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class LinStor
{
    // ============================================================
    // Product and version information
    //
    public static final String PROGRAM = "LINSTOR";
    public static final String VERSION = "0.1.0-experimental [2017-10-24_001]";

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

    // Database drivers
    protected static DbAccessor securityDbDriver;
    protected static DatabaseDriver persistenceDbDriver;

    // Error & exception logging facility
    private ErrorReporter errorLog;

    LinStor()
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

    public void startSystemServices(Iterable<SystemService> services)
    {
        ErrorReporter errLog = getErrorReporter();
        // Start services
        for (SystemService sysSvc : services)
        {
            errLog.logInfo(
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
                errLog.reportProblem(Level.ERROR, startExc, null, null, null);
            }
            catch (Exception unhandledExc)
            {
                errLog.reportError(unhandledExc);
            }
        }
    }

    public void stopSystemServices(Iterable<SystemService> services)
    {
        ErrorReporter errLog = getErrorReporter();
        // Shutdown services
        for (SystemService sysSvc : services)
        {
            errLog.logInfo(
                String.format(
                    "Shutting down service instance '%s' of type %s",
                    sysSvc.getInstanceName().displayValue, sysSvc.getServiceName().displayValue
                )
            );
            try
            {
                sysSvc.shutdown();

                errLog.logInfo(
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
        out.printf("    %-24s %s\n", title, text);
    }

    /**
     * Returns classpath entries expanded and absolute.
     *
     * @param classpath to expand entries from
     * @return List of classpath entries
     */
    public static List<String> expandClassPath(String classpath)
    {
        final ArrayList<String> paths = new ArrayList<>();
        for(String sp : classpath.split(File.pathSeparator))
        {
            Path p = Paths.get(sp);
            // make path absolute
            if(!p.isAbsolute())
            {
                p = p.toAbsolutePath();
            }

            // check if path contains wildcard
            // java classpath wildcards are no standard and to current knowledge limited to '*'
            if(p.toString().contains("*"))
            {
                if(p.getFileName().toString().equals("*"))
                {
                    String glob = "glob:" + p.getFileName();
                    final PathMatcher matcher = FileSystems.getDefault().getPathMatcher(glob);
                    for(File file : p.getParent().toFile().listFiles())
                    {
                        if(matcher.matches(file.toPath().getFileName())
                                && (file.toString().endsWith(".jar") || file.toString().endsWith(".JAR")))
                        {
                            paths.add(file.toString());
                        }
                    }
                }
            }
            else
            {
                paths.add(p.toString());
            }
        }

        return paths;
    }

    /**
     * Loads implementations of the {@link ApiCall} from the package
     * where {@link BaseApiCall} is currently placed + ".common"
     * and stores them into msgProc.
     *
     * If the componentRef's class equals {@link Controller}, then the ApiCall
     * classes from the {@link BaseApiCall}'s package + ".controller" are loaded.
     * Otherwise the ".satellite" package is loaded additionally.
     *
     * The parameter componentRef is used to initialize the ApiCall.
     *
     * @param msgProc
     * @param componentRef
     * @param coreService
     * @param apiType
     */
    protected static void loadApiCalls(
        final CommonMessageProcessor msgProc,
        final LinStor componentRef,
        final CoreServices coreService,
        final ApiType apiType
    )
    {
        final ClassLoader cl = componentRef.getClass().getClassLoader();

        String[] pkgsToload;
        String basePackage = apiType.getBasePackageName();
        String commonPkg = basePackage + ".common";

        if (componentRef.getClass().equals(Controller.class))
        {
            pkgsToload = new String[]
            {
                commonPkg,
                basePackage + ".controller"
            };
        }
        else
        {
            pkgsToload = new String[]
            {
                commonPkg,
                basePackage + ".satellite"
            };
        }

        List<String> loadPaths = expandClassPath(System.getProperty("java.class.path"));
        int loadedClasses = 0;
        for(String loadPath : loadPaths)
        {
            final Path basePath = Paths.get(loadPath);
            if(Files.isDirectory(basePath))
            {
                loadedClasses += loadApiCallsFromDirectory(msgProc, componentRef, coreService, cl, apiType, basePath, pkgsToload);
            }
            else // must be a jar file
            {
                loadedClasses += loadApiCallsFromJar(msgProc, componentRef, coreService, cl, apiType, basePath, pkgsToload);
            }
        }

        if(loadedClasses == 0)
        {
            componentRef.errorLog.logWarning(
                "No api classes were found in classpath."
            );
        }
    }

    /**
     * Load api calls from the given directory path.
     * @param msgProc
     * @param componentRef
     * @param coreService
     * @param cl
     * @param apiType
     * @param directoryPath
     * @param pkgsToload
     */
    private static int loadApiCallsFromDirectory(
        final CommonMessageProcessor msgProc,
        final LinStor componentRef,
        final CoreServices coreService,
        final ClassLoader cl,
        final ApiType apiType,
        final Path directoryPath,
        final String[] pkgsToload
    )
    {
        // this is an ugly hack to get a mutable counter
        // all other ways to do this would result in exposing the static loadApiCalls methods
        // to public and create wrapper classes for the FileVisitor
        final int[] loadedClasses = {0};
        for (final String pkgToLoad : pkgsToload)
        {
            Path pkgPath = Paths.get(pkgToLoad.replaceAll("\\.", File.separator));
            pkgPath = directoryPath.resolve(pkgPath);

            if (pkgPath.toFile().exists())
            try
            {
                Files.walkFileTree(directoryPath.resolve(pkgPath), new SimpleFileVisitor<Path>()
                {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
                    {
                        if(loadClassFromFile(msgProc, componentRef, coreService, cl, directoryPath, pkgToLoad, file, apiType))
                        {
                            loadedClasses[0]++;
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
            catch (IOException ioExc)
            {
                componentRef.errorLog.reportError(
                    new LinStorException(
                        "Failed to load classes from " + pkgPath,
                        "See cause for more details",
                        ioExc.getLocalizedMessage(),
                        null,
                        null,
                        ioExc
                    )
                );
            }
        }
        return loadedClasses[0];
    }

    /**
     * Load api calls from the given jar file.
     * @param msgProc
     * @param componentRef
     * @param coreService
     * @param cl
     * @param apiType
     * @param jarPath
     * @param pkgsToload
     */
    private static int loadApiCallsFromJar(
        final CommonMessageProcessor msgProc,
        final LinStor componentRef,
        final CoreServices coreService,
        final ClassLoader cl,
        final ApiType apiType,
        final Path jarPath,
        final String[] pkgsToload
    )
    {
        int loadedClasses = 0;
        if(!jarPath.toString().toLowerCase().endsWith(".jar"))
        {
            return loadedClasses;
        }

        try (JarFile jarFile = new JarFile(jarPath.toFile())) {
            Enumeration<JarEntry> e = jarFile.entries();

            while(e.hasMoreElements())
            {
                JarEntry je = e.nextElement();

                for (final String pkgToLoad : pkgsToload)
                {
                    Path pkgPath = Paths.get(pkgToLoad.replaceAll("\\.", File.separator));

                    if(je.getName().startsWith(pkgPath.toString()) && je.getName().endsWith(".class"))
                    {
                        String fullQualifiedClassName = je.getName().replaceAll(File.separator, ".");
                        fullQualifiedClassName = fullQualifiedClassName.substring(0, fullQualifiedClassName.lastIndexOf(".")); // cut the ".class"
                        if(loadClass(msgProc, componentRef, coreService, cl, pkgToLoad, fullQualifiedClassName, apiType))
                        {
                            loadedClasses++;
                        }
                    }
                }
            }
        }
        catch (IOException ioExc)
        {
            componentRef.errorLog.reportError(
                new LinStorException(
                    "Failed to load classes from " + jarPath,
                    "See cause for more details",
                    ioExc.getLocalizedMessage(),
                    null,
                    null,
                    ioExc
                )
            );
        }

        return loadedClasses;
    }

    /**
     * Load the given class file.
     * @param msgProc
     * @param componentRef
     * @param coreService
     * @param cl
     * @param basePath
     * @param pkgToLoad
     * @param file
     * @param apiType
     */
    private static boolean loadClassFromFile(
        final CommonMessageProcessor msgProc,
        final LinStor componentRef,
        final CoreServices coreService,
        final ClassLoader cl,
        final Path basePath,
        final String pkgToLoad,
        Path file,
        final ApiType apiType
    )
    {
        if (file.getFileName().toString().endsWith(".class"))
        {
            if (file.isAbsolute())
            {
                file = basePath.relativize(file);
            }
            String fullQualifiedClassName = file.toString().replaceAll(File.separator, ".");
            fullQualifiedClassName = fullQualifiedClassName.substring(0, fullQualifiedClassName.lastIndexOf(".")); // cut the ".class"
            return loadClass(msgProc, componentRef, coreService, cl, pkgToLoad, fullQualifiedClassName, apiType);
        }
        return false;
    }

    /**
     * Load and check the given fullQualifiedClassName.
     * If all signature tests are passed the loaded class will be added to the msgProc apicalls
     *
     * @param msgProc
     * @param componentRef
     * @param coreService
     * @param cl
     * @param pkgToLoad
     * @param fullQualifiedClassName
     * @param apiType
     * @return true if class was loaded.
     */
    private static boolean loadClass(
        final CommonMessageProcessor msgProc,
        final LinStor componentRef,
        final CoreServices coreService,
        final ClassLoader cl,
        final String pkgToLoad,
        final String fullQualifiedClassName,
        final ApiType apiType
    )
    {
        boolean classLoaded = false;
        Class<?> clazz = null;
        try
        {
            clazz = cl.loadClass(fullQualifiedClassName);
        }
        catch (ClassNotFoundException e)
        {
            componentRef.errorLog.reportProblem(Level.DEBUG,
                new LinStorException(
                    "Dynamic loading of API classes threw ClassNotFoundException",
                    String.format(
                        "Loading the class '%s' resulted in a ClassNotFoundException",
                        fullQualifiedClassName),
                    String.format(
                        "While loading all classes from package '%s', the class '%s' could not be laoded",
                        pkgToLoad,
                        fullQualifiedClassName),
                    null,
                    null,
                    e),
                null, // accCtx
                null, // client
                null  // contextInfo
            );
        }

        if (clazz != null && clazz.getAnnotation(apiType.getRequiredAnnotation()) != null)
        {
            if (Modifier.isAbstract(clazz.getModifiers()))
            {
                String message = String.format(
                    "Skipping dynamic loading of api class '%s' as it is abstract",
                    fullQualifiedClassName
                );
                componentRef.errorLog.reportError(
                    new LinStorException(
                        message,
                        message,
                        "Cannot instantiate abstract class",
                        "Make the class instantiable or move the class from the api package",
                        null
                    )
                );

            }
            else
            if (!ApiCall.class.isAssignableFrom(clazz))
            {
                String message = String.format(
                    "Skipping dynamic loading of api class '%s' as it does not implement ApiCall",
                    fullQualifiedClassName
                );
                componentRef.errorLog.reportError(
                    new LinStorException(
                        message,
                        message,
                        null,
                        "Let the class implement ApiCall or move the class from the api package",
                        null
                    )
                );
            }
            else
            {
                Constructor<?>[] declaredConstructors = clazz.getDeclaredConstructors();
                Constructor<?> ctor = null;
                for (Constructor<?> declaredConstructor : declaredConstructors)
                {
                    Class<?>[] parameterTypes = declaredConstructor.getParameterTypes();

                    if (Modifier.isPublic(declaredConstructor.getModifiers()))
                    {
                        if ((parameterTypes.length == 2 &&
                            parameterTypes[0].isAssignableFrom(componentRef.getClass()) &&
                            parameterTypes[1].isAssignableFrom(CoreServices.class)) ||

                            parameterTypes.length == 1 &&
                            parameterTypes[0].isAssignableFrom(componentRef.getClass()) ||

                            parameterTypes.length == 0)
                        {
                            ctor = declaredConstructor;
                            break;
                        }
                    }
                }
                if (ctor == null)
                {
                    String message = String.format(
                        "Skipping dynamic loading of api class '%s' as it does not contain the expected constructor",
                        fullQualifiedClassName
                    );
                    componentRef.errorLog.reportError(
                        new LinStorException(
                            message,
                            message,
                            null,
                            String.format(
                                "Create a public constructor with the parameters %s and %s",
                                componentRef.getClass().getName(),
                                CoreServices.class.getName()
                            ),
                            null
                        )
                    );
                }
                else
                {
                    Object instance = null;
                    try
                    {
                        switch (ctor.getParameterTypes().length) {
                            case 2:
                                instance = ctor.newInstance(componentRef, coreService);
                                break;
                            case 1:
                                instance = ctor.newInstance(componentRef);
                                break;
                            case 0:
                                instance = ctor.newInstance();
                                break;
                            default:
                                componentRef.errorLog.reportError(
                                        new ImplementationError("Unexpected API class constructor", null)
                                );  break;
                        }
                    }
                    catch (
                        InstantiationException | IllegalAccessException |
                        IllegalArgumentException | InvocationTargetException exc
                    )
                    {
                        String message = String.format(
                            "Creating new instance of %s failed. See cause for more details",
                            fullQualifiedClassName
                        );
                        componentRef.errorLog.reportError(
                            new LinStorException(
                                message,
                                message,
                                exc.getLocalizedMessage(),
                                null,
                                null,
                                exc
                            )
                        );
                    }
                    if (instance != null)
                    {
                        try
                        {
                            ApiCall baseInstance = (ApiCall) instance;
                            msgProc.addApiCall(baseInstance);
                            componentRef.errorLog.logTrace("Loaded API class: " + fullQualifiedClassName);
                            classLoaded = true;
                        }
                        catch (ClassCastException ccExc)
                        {
                            // technically this should never occur
                            componentRef.errorLog.reportError(
                                new ImplementationError(
                                    "Previous checks if dynamically loaded api class is castable failed",
                                    ccExc
                                )
                            );
                        }
                    }
                }
            }
        }
        return classLoaded;
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

    // static Database reference getters

    public static PropsConDatabaseDriver getPropConDatabaseDriver()
    {
        return persistenceDbDriver.getPropsDatabaseDriver();
    }

    public static NodeDataDatabaseDriver getNodeDataDatabaseDriver()
    {
        return persistenceDbDriver.getNodeDatabaseDriver();
    }

    public static ObjectProtectionDatabaseDriver getObjectProtectionDatabaseDriver()
    {
        return securityDbDriver.getObjectProtectionDatabaseDriver();
    }

    public static ResourceDataDatabaseDriver getResourceDataDatabaseDriver()
    {
        return persistenceDbDriver.getResourceDataDatabaseDriver();
    }

    public static ResourceDefinitionDataDatabaseDriver getResourceDefinitionDataDatabaseDriver()
    {
        return persistenceDbDriver.getResourceDefinitionDataDatabaseDriver();
    }

    public static VolumeDataDatabaseDriver getVolumeDataDatabaseDriver()
    {
        return persistenceDbDriver.getVolumeDataDatabaseDriver();
    }

    public static VolumeDefinitionDataDatabaseDriver getVolumeDefinitionDataDatabaseDriver()
    {
        return persistenceDbDriver.getVolumeDefinitionDataDatabaseDriver();
    }

    public static StorPoolDataDatabaseDriver getStorPoolDataDatabaseDriver()
    {
        return persistenceDbDriver.getStorPoolDataDatabaseDriver();
    }

    public static StorPoolDefinitionDataDatabaseDriver getStorPoolDefinitionDataDatabaseDriver()
    {
        return persistenceDbDriver.getStorPoolDefinitionDataDatabaseDriver();
    }

    public static NetInterfaceDataDatabaseDriver getNetInterfaceDataDatabaseDriver()
    {
        return persistenceDbDriver.getNetInterfaceDataDatabaseDriver();
    }

    public static NodeConnectionDataDatabaseDriver getNodeConnectionDatabaseDriver()
    {
        return persistenceDbDriver.getNodeConnectionDataDatabaseDriver();
    }

    public static ResourceConnectionDataDatabaseDriver getResourceConnectionDatabaseDriver()
    {
        return persistenceDbDriver.getResourceConnectionDataDatabaseDriver();
    }

    public static VolumeConnectionDataDatabaseDriver getVolumeConnectionDatabaseDriver()
    {
        return persistenceDbDriver.getVolumeConnectionDataDatabaseDriver();
    }

    public abstract void setSecurityLevel(AccessContext accCtx, SecurityLevel newLevel)
        throws AccessDeniedException, SQLException;
}
