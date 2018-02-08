package com.linbit.linstor.security;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.linbit.linstor.logging.ErrorReporter;

public class SecurityModule extends AbstractModule
{
    private final AccessContext initCtx;

    public SecurityModule(AccessContext initCtx)
    {
        this.initCtx = initCtx;
    }

    @Override
    protected void configure()
    {

    }

    @Provides
    @Singleton
    public DbAccessor securityDbDriver(ErrorReporter errorLogRef)
    {
        return new DbDerbyPersistence(initCtx, errorLogRef);
    }
}
