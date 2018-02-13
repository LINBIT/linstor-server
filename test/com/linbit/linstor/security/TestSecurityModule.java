package com.linbit.linstor.security;

import com.google.inject.AbstractModule;
import com.linbit.linstor.annotation.SystemContext;

public class TestSecurityModule extends AbstractModule
{
    private final AccessContext initCtx;

    public TestSecurityModule(AccessContext initCtxRef)
    {
        initCtx = initCtxRef;
    }

    @Override
    protected void configure()
    {
        bind(AccessContext.class).annotatedWith(SystemContext.class).toInstance(initCtx);
    }
}
