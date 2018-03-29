package com.linbit.linstor.dbcp.migration;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Provides migration info in the form of an annotation.
 */
@Retention(RUNTIME)
@Target(TYPE)
public @interface Migration
{
    /**
     * The migration version in dotted timestamp format <code>yyyy.MM.dd.HH.mm</code>.
     * This format allows migrations to be created in branches without collisions.
     */
    String version();

    /**
     * An informative description of the migration.
     * The description is used for verification and hence should not be changed once the migration has been released.
     */
    String description();
}
