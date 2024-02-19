package com.linbit.linstor.spacetracking;

import com.linbit.SystemService;
import com.linbit.linstor.core.Controller;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class DefaultSpaceTrackingModule  extends AbstractModule
{
    public DefaultSpaceTrackingModule()
    {
    }

    @Override
    protected void configure()
    {
        bind(SpaceTrackingService.class).toProvider(() -> null);
        bind(SystemService.class).annotatedWith(Names.named(Controller.SPC_TRK_MODULE_NAME))
            .toProvider(() -> null);
    }
}
