package com.linbit.linstor.dbcp.migration.k8s.crd;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target(TYPE)
public @interface K8sCrdMigration
{
    String description();

    int version();
}
