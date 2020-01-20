package com.linbit.linstor.core;

import com.linbit.linstor.core.cfg.StltConfig;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class SatelliteArgumentsModule extends AbstractModule
{
    private final StltConfig stltCfg;

    public SatelliteArgumentsModule(StltConfig stltCfgRef)
    {
        stltCfg = stltCfgRef;
    }

    @Provides
    StltConfig provideStltConfig()
    {
        return stltCfg;
    }
}
