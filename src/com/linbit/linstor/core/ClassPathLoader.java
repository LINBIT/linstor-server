package com.linbit.linstor.core;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.logging.ErrorReporter;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
     */
    public <T> List<Class<? extends T>> loadClasses(
        String basePackage,
        List<String> packageSuffixes,
        Class<T> requiredClass,
        Class<? extends Annotation> requiredAnnotation
    )
    {
        List<Class<? extends T>> classes = Collections.emptyList();
        try
        {
            ClassPath classPath = ClassPath.from(ClassPathLoader.class.getClassLoader());

            classes = packageSuffixes.stream()
                .map(packageSuffix -> basePackage + "." + packageSuffix)
                .map(classPath::getTopLevelClasses)
                .flatMap(ImmutableSet::stream)
                .map(ClassPath.ClassInfo::load)
                .map(clazz -> ClassPathLoader.<T>asClass(requiredClass, clazz))
                .filter(Objects::nonNull)
                .filter(clazz -> clazz.getAnnotation(requiredAnnotation) != null)
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

        if (classes.isEmpty())
        {
            errorReporter.logWarning(
                "No api classes were found in classpath."
            );
        }

        return classes;
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<? extends T> asClass(Class<T> requiredClass, Class<?> clazz)
    {
        return requiredClass.isAssignableFrom(clazz) ? (Class<? extends T>) clazz : null;
    }
}
