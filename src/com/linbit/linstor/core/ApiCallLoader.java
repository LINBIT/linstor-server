package com.linbit.linstor.core;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.logging.ErrorReporter;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ApiCallLoader
{
    private final ErrorReporter errorReporter;

    public ApiCallLoader(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
    }

    /**
     * Returns classpath entries expanded and absolute.
     *
     * @param classpath to expand entries from
     * @return List of classpath entries
     */
    public List<String> expandClassPath(String classpath)
    {
        final ArrayList<String> paths = new ArrayList<>();
        for (String pathItems : classpath.split(File.pathSeparator))
        {
            Path clPath = Paths.get(pathItems);
            // make path absolute
            if (!clPath.isAbsolute())
            {
                clPath = clPath.toAbsolutePath();
            }

            // check if path contains wildcard
            // java classpath wildcards are no standard and to current knowledge limited to '*'
            if (clPath.toString().contains("*"))
            {
                if (clPath.getFileName().toString().equals("*"))
                {
                    String glob = "glob:" + clPath.getFileName();
                    final PathMatcher matcher = FileSystems.getDefault().getPathMatcher(glob);
                    for (File file : clPath.getParent().toFile().listFiles())
                    {
                        if (matcher.matches(file.toPath().getFileName()) &&
                            (file.toString().endsWith(".jar") || file.toString().endsWith(".JAR")))
                        {
                            paths.add(file.toString());
                        }
                    }
                }
            }
            else
            {
                paths.add(clPath.toString());
            }
        }

        return paths;
    }

    /**
     * Loads implementations of the {@link ApiCall}.
     *
     * The packages to search are determined by appending each of the given suffixes onto the base package name
     * given by the {@link ApiType}.
     */
    public List<Class<? extends ApiCall>> loadApiCalls(final ApiType apiType, List<String> packageSuffixes)
    {
        final ClassLoader cl = getClass().getClassLoader();

        List<String> pkgsToload = new ArrayList<>();
        String basePackage = apiType.getBasePackageName();

        for (String packageSuffix : packageSuffixes)
        {
            pkgsToload.add(basePackage + "." + packageSuffix);
        }

        List<String> loadPaths = expandClassPath(System.getProperty("java.class.path"));
        List<Class<? extends ApiCall>> classes = new ArrayList<>();
        for (String loadPath : loadPaths)
        {
            final Path basePath = Paths.get(loadPath);
            if (Files.isDirectory(basePath))
            {
                classes.addAll(loadApiCallsFromDirectory(
                    cl,
                    apiType,
                    basePath,
                    pkgsToload
                ));
            }
            else // must be a jar file
            {
                classes.addAll(loadApiCallsFromJar(
                    cl,
                    apiType,
                    basePath,
                    pkgsToload
                ));
            }
        }

        if (classes.isEmpty())
        {
            errorReporter.logWarning(
                "No api classes were found in classpath."
            );
        }

        return classes;
    }

    /**
     * Load api calls from the given directory path.
     */
    @SuppressWarnings("checkstyle:descendanttoken")
    // checkstyle complains about the return of the inner-class-method
    private List<Class<? extends ApiCall>> loadApiCallsFromDirectory(
        final ClassLoader cl,
        final ApiType apiType,
        final Path directoryPath,
        final List<String> pkgsToload
    )
    {
        final List<Class<? extends ApiCall>> loadedClasses = new ArrayList<>();
        for (final String pkgToLoad : pkgsToload)
        {
            Path pkgPath = Paths.get(pkgToLoad.replaceAll("\\.", File.separator));
            pkgPath = directoryPath.resolve(pkgPath);

            if (pkgPath.toFile().exists())
            {
                try
                {
                    Files.walkFileTree(directoryPath.resolve(pkgPath), new SimpleFileVisitor<Path>()
                    {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
                        {
                            Class<? extends ApiCall> clazz = loadClassFromFile(
                                cl,
                                directoryPath,
                                pkgToLoad,
                                file,
                                apiType
                            );

                            if (clazz != null)
                            {
                                loadedClasses.add(clazz);
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
                }
                catch (IOException ioExc)
                {
                    errorReporter.reportError(
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
        }
        return loadedClasses;
    }

    /**
     * Load api calls from the given jar file.
     */
    private List<Class<? extends ApiCall>> loadApiCallsFromJar(
        final ClassLoader cl,
        final ApiType apiType,
        final Path jarPath,
        final List<String> pkgsToload
    )
    {
        final List<Class<? extends ApiCall>> loadedClasses = new ArrayList<>();
        if (jarPath.toString().toLowerCase().endsWith(".jar"))
        {
            try (JarFile jarFile = new JarFile(jarPath.toFile()))
            {
                Enumeration<JarEntry> entry = jarFile.entries();

                while (entry.hasMoreElements())
                {
                    JarEntry je = entry.nextElement();

                    for (final String pkgToLoad : pkgsToload)
                    {
                        Path pkgPath = Paths.get(pkgToLoad.replaceAll("\\.", File.separator));

                        if (je.getName().startsWith(pkgPath.toString()) && je.getName().endsWith(".class"))
                        {
                            String fullQualifiedClassName = je.getName().replaceAll(File.separator, ".");
                            fullQualifiedClassName = fullQualifiedClassName.substring(
                                0,
                                fullQualifiedClassName.lastIndexOf('.') // cut the ".class"
                            );
                            Class<? extends ApiCall> clazz = loadClass(
                                cl,
                                pkgToLoad,
                                fullQualifiedClassName,
                                apiType
                            );
                            if (clazz != null)
                            {
                                loadedClasses.add(clazz);
                            }
                        }
                    }
                }
            }
            catch (IOException ioExc)
            {
                errorReporter.reportError(
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
        }
        return loadedClasses;
    }

    /**
     * Load the given class file.
     */
    private Class<? extends ApiCall> loadClassFromFile(
        final ClassLoader cl,
        final Path basePath,
        final String pkgToLoad,
        Path fileRef,
        final ApiType apiType
    )
    {
        Class<? extends ApiCall> ret = null;
        Path file = fileRef;
        if (file.getFileName().toString().endsWith(".class"))
        {
            if (file.isAbsolute())
            {
                file = basePath.relativize(file);
            }
            String fullQualifiedClassName = file.toString().replaceAll(File.separator, ".");
            fullQualifiedClassName = fullQualifiedClassName.substring(
                0,
                fullQualifiedClassName.lastIndexOf('.') // cut the ".class"
            );
            ret = loadClass(cl, pkgToLoad, fullQualifiedClassName, apiType);
        }
        return ret;
    }

    /**
     * Load and check the given fullQualifiedClassName.
     *
     * @return null if class could not be loaded.
     */
    private Class<? extends ApiCall> loadClass(
        final ClassLoader cl,
        final String pkgToLoad,
        final String fullQualifiedClassName,
        final ApiType apiType
    )
    {
        Class<?> clazz = null;
        Class<? extends ApiCall> apiCallClazz = null;
        try
        {
            clazz = cl.loadClass(fullQualifiedClassName);
        }
        catch (ClassNotFoundException exc)
        {
            errorReporter.reportProblem(Level.DEBUG,
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
                    exc
                ),
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
                errorReporter.reportError(
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
            {
                apiCallClazz = castToApiCall(clazz);
            }
        }
        return apiCallClazz;
    }

    @SuppressWarnings("unchecked")
    private Class<? extends ApiCall> castToApiCall(Class<?> clazz)
    {
        Class<? extends ApiCall> apiCallClazz = null;

        if (clazz != null)
        {
            if (ApiCall.class.isAssignableFrom(clazz))
            {
                apiCallClazz = (Class<? extends ApiCall>) clazz;
            }
            else
            {
                String message = String.format(
                    "Skipping dynamic loading of api class '%s' as it does not implement ApiCall",
                    clazz.getName()
                );
                errorReporter.reportError(
                    new LinStorException(
                        message,
                        message,
                        null,
                        "Let the class implement ApiCall or move the class from the api package",
                        null
                    )
                );
            }
        }

        return apiCallClazz;
    }
}
