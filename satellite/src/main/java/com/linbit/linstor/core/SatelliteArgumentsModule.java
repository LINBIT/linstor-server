package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class SatelliteArgumentsModule extends AbstractModule
{
    private final SatelliteCmdlArguments args;

    public SatelliteArgumentsModule(SatelliteCmdlArguments argsRef)
    {
        args = argsRef;
    }

    @Provides
    SatelliteCmdlArguments provideSatelliteCmdlArguments()
    {
        return args;
    }
}
