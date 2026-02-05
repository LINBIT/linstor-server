package com.linbit.linstor.dbcp.migration.k8s.crd;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(TYPE)
public @interface K8sCrdMigration
{
    String description();

    int version();

    boolean isInitial() default false;
}
