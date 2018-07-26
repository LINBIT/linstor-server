package com.linbit.linstor.event;

import com.google.inject.AbstractModule;
import com.linbit.linstor.event.generator.ResourceDeploymentStateGenerator;
import com.linbit.linstor.event.generator.ResourceStateGenerator;
import com.linbit.linstor.event.generator.VolumeDiskStateGenerator;
import com.linbit.linstor.event.generator.satellite.StltResourceDeploymentStateGenerator;
import com.linbit.linstor.event.generator.satellite.StltResourceStateGenerator;
import com.linbit.linstor.event.generator.satellite.StltVolumeDiskStateGenerator;

public class SatelliteEventModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(VolumeDiskStateGenerator.class).to(StltVolumeDiskStateGenerator.class);
        bind(ResourceStateGenerator.class).to(StltResourceStateGenerator.class);
        bind(ResourceDeploymentStateGenerator.class).to(StltResourceDeploymentStateGenerator.class);
    }
}
