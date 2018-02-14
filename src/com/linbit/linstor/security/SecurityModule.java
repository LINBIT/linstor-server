package com.linbit.linstor.security;

import com.google.inject.AbstractModule;
import com.linbit.linstor.annotation.SystemContext;

public class SecurityModule extends AbstractModule
{
    public static final String SHUTDOWN_PROT = "shutdownProt";

    private final AccessContext systemCtx;

    public SecurityModule(AccessContext systemCtxRef)
    {
        systemCtx = systemCtxRef;
    }

    @Override
    protected void configure()
    {
        bind(AccessContext.class).annotatedWith(SystemContext.class).toInstance(systemCtx);
    }
}
