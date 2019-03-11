package com.linbit.linstor.security;

import com.google.inject.AbstractModule;

import com.linbit.linstor.annotation.PublicContext;
import com.linbit.linstor.annotation.SystemContext;

import static com.linbit.linstor.security.GenericDbBase.PUBLIC_CTX;

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
        bind(AccessContext.class).annotatedWith(PublicContext.class).toInstance(PUBLIC_CTX);
    }
}
