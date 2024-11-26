package com.linbit.linstor.core.migration;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.ClassPathLoader;
import com.linbit.linstor.logging.ErrorReporter;

import javax.annotation.Nullable;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;

public class SatelliteMigrationsModule extends AbstractModule
{
    public SatelliteMigrationsModule()
    {
    }

    @Provides
    @Singleton
    public Map<SatelliteMigrations, BaseStltMigration> getAllStltMigrations(
        Injector injector,
        ErrorReporter errorReporterRef
    )
    {
        Map<SatelliteMigrations, BaseStltMigration> ret = new EnumMap<>(SatelliteMigrations.class);

        ClassPathLoader classPathLoader = new ClassPathLoader(errorReporterRef);
        List<Class<? extends BaseStltMigration>> stltMigrationClasses = classPathLoader.loadClasses(
            BaseStltMigration.class.getPackage().getName(),
            Collections.singletonList("migrations"),
            BaseStltMigration.class,
            StltMigration.class
        );

        List<Class<? extends BaseStltMigration>> stltMigClassesMissingAnnotation = new ArrayList<>();
        for (Class<? extends BaseStltMigration> stltMigCls : stltMigrationClasses)
        {
            @Nullable StltMigration annot = stltMigCls.getAnnotation(StltMigration.class);
            if (annot == null)
            {
                stltMigClassesMissingAnnotation.add(stltMigCls);
            }
            else
            {
                ret.put(annot.migration(), injector.getInstance(stltMigCls));
            }
        }

        sanityChecks(ret, stltMigClassesMissingAnnotation);

        return ret;
    }

    private void sanityChecks(
        Map<SatelliteMigrations, BaseStltMigration> retRef,
        List<Class<? extends BaseStltMigration>> stltMigClassesMissingAnnotationRef
    )
    {
        sanityCheckMigrationsMissingAnnotations(stltMigClassesMissingAnnotationRef);
        sanityCheckUnhandledMigration(retRef);
    }

    private void sanityCheckUnhandledMigration(Map<SatelliteMigrations, BaseStltMigration> allMigrationsMap)
    {
        List<SatelliteMigrations> missingEntries = new ArrayList<>();
        // the order does not matter in this case, so we use the actual enum's "values()" method just
        // to make really really sure that we do not miss somehow a value
        for (SatelliteMigrations stltMig : SatelliteMigrations.values())
        {
            if (!allMigrationsMap.containsKey(stltMig))
            {
                missingEntries.add(stltMig);
            }
        }
        if (!missingEntries.isEmpty())
        {
            StringBuilder sb = new StringBuilder("The following satellite migrations are missing:");
            for (SatelliteMigrations stltMig : missingEntries)
            {
                sb.append("\n").append(stltMig.name());
            }
            throw new ImplementationError(sb.toString());
        }
    }

    private void sanityCheckMigrationsMissingAnnotations(
        List<Class<? extends BaseStltMigration>> stltMigClassesMissingAnnotationRef
    )
    {
        if (!stltMigClassesMissingAnnotationRef.isEmpty())
        {
            StringBuilder sb = new StringBuilder("The following satellite migrations are missing the annotation @")
                .append(StltMigration.class.getSimpleName())
                .append(":");
            for (Class<? extends BaseStltMigration> stltMigCls : stltMigClassesMissingAnnotationRef)
            {
                sb.append("\n  simple name: ")
                    .append(stltMigCls.getSimpleName())
                    .append(", canonical name: ")
                    .append(stltMigCls.getCanonicalName());
            }
            throw new ImplementationError(sb.toString());
        }
    }
}
