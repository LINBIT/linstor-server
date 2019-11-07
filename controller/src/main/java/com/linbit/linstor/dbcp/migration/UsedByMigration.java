package com.linbit.linstor.dbcp.migration;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PACKAGE;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.SOURCE;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * DO NOT MODIFY THIS CLASS / METHOD
 * <br/>
 * This annotation marks the class or method to be used by migrations.
 * They MUST NOT CHANGE their output or migrations might break!
 */
@Retention(SOURCE)
@Target(
    {
        TYPE, FIELD, METHOD, CONSTRUCTOR, PACKAGE
    }
)
public @interface UsedByMigration
{

}
