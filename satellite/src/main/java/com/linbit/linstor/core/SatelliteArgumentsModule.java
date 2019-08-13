package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class SatelliteArgumentsModule extends AbstractModule
{
    private final SatelliteCmdlArguments args;
    private final SatelliteConfigToml stltConfig;

    public SatelliteArgumentsModule(SatelliteCmdlArguments argsRef, SatelliteConfigToml stltConfigRef)
    {
        args = argsRef;
        stltConfig = stltConfigRef;
    }

    @Provides
    SatelliteCmdlArguments provideSatelliteCmdlArguments()
    {
        return args;
    }

    @Provides
    SatelliteConfigToml provideSatelliteConfigToml()
    {
        return stltConfig;
    }
}
