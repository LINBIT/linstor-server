package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class ControllerArgumentsModule extends AbstractModule
{
    private final ControllerCmdlArguments args;
    private final LinstorConfigToml linstorConfigToml;

    public ControllerArgumentsModule(ControllerCmdlArguments argsRef, LinstorConfigToml linstorConfigTomlRef)
    {
        args = argsRef;
        linstorConfigToml = linstorConfigTomlRef;
    }

    @Provides
    ControllerCmdlArguments provideControllerCmdlArguments()
    {
        return args;
    }

    @Provides
    LinstorConfigToml provideLinstorConfig()
    {
        return linstorConfigToml;
    }
}
