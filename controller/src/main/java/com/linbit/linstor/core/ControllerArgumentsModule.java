package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class ControllerArgumentsModule extends AbstractModule
{
    private final ControllerCmdlArguments args;

    public ControllerArgumentsModule(ControllerCmdlArguments argsRef)
    {
        args = argsRef;
    }

    @Provides
    ControllerCmdlArguments provideControllerCmdlArguments()
    {
        return args;
    }
}
