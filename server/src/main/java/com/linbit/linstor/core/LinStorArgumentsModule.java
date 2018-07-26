package com.linbit.linstor.core;

import com.google.inject.AbstractModule;

public class LinStorArgumentsModule extends AbstractModule
{
    private final LinStorArguments args;

    public LinStorArgumentsModule(LinStorArguments argsRef)
    {
        args = argsRef;
    }

    @Override
    protected void configure()
    {
        bind(LinStorArguments.class).toInstance(args);
    }
}
