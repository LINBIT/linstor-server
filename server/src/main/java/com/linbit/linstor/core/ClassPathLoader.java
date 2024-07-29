package com.linbit.linstor.core;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.logging.ErrorReporter;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.reflect.ClassPath;

public class ClassPathLoader
{
    private final ErrorReporter errorReporter;

    public ClassPathLoader(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
    }

    /**
     * Load implementations of the given class which have the required annotation.
     *
     * The packages to search are determined by appending each of the given suffixes onto the base package name.
     *
     * @param basePackage The base package which will be expanded with the elements of packageSuffixes.
     *  E.g. "org.example"
     * @param packageSuffixes The suffixes that get concatenated with the base package.
     *  E.g ["foo", "bar"] -> ["org.example.foo", "org.example.bar"]
     * @param requiredClass The base class every top-level class of the package(s) have to extend
     *  (directly or indirectly)
     * @param requiredAnnotation The annotation every top-level class has to have
     */
    public <T> List<Class<? extends T>> loadClasses(
        String basePackage,
        List<String> packageSuffixes,
        @Nullable Class<T> requiredClass,
        Class<? extends Annotation> requiredAnnotation
    )
    {
        List<Class<? extends T>> classes = packageSuffixes.stream()
            .map(packageSuffix -> packageSuffix.isEmpty() ? basePackage : (basePackage + "." + packageSuffix))
            .map(fullQualPkgName -> loadClasses(fullQualPkgName, requiredClass))
            .flatMap(List::stream)
            .filter(clazz -> clazz.getAnnotation(requiredAnnotation) != null)
            .collect(Collectors.toList());

        if (classes.isEmpty())
        {
            errorReporter.logWarning(
                "No api classes were found in classpath."
            );
        }

        return classes;
    }

    /**
     * Loads all classes from a single package
     *
     * @param pkgName The package all top level classes should be loaded from
     * @param requiredClass The base class all classes have to extend (directly or indirectly)
     */
    public <T> List<Class<? extends T>> loadClasses(
        String pkgName,
        @Nullable Class<T> requiredClass
    )
    {
        List<Class<? extends T>> classes = Collections.emptyList();
        try
        {
            Function<Class<?>, Class<? extends T>> clsMapper;
            if (requiredClass != null)
            {
                clsMapper = clazz -> ClassPathLoader.<T>asClass(requiredClass, clazz);
            }
            else
            {
                clsMapper = clazz -> (Class<? extends T>) clazz;
            }
            classes = ClassPath.from(ClassPathLoader.class.getClassLoader())
                .getTopLevelClasses(pkgName)
                .stream()
                .map(ClassPath.ClassInfo::load)
                .map(clsMapper)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(
                new LinStorException(
                    "Failed to load classes",
                    "See cause for more details",
                    ioExc.getLocalizedMessage(),
                    null,
                    null,
                    ioExc
                )
            );
        }
        return classes;
    }

    @SuppressWarnings("unchecked")
    private static <T> @Nullable Class<? extends T> asClass(Class<T> requiredClass, Class<?> clazz)
    {
        return requiredClass.isAssignableFrom(clazz) ? (Class<? extends T>) clazz : null;
    }
}
