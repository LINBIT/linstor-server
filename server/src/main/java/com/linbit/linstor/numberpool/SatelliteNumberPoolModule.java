package com.linbit.linstor.numberpool;

import static com.linbit.linstor.numberpool.NumberPoolModule.LAYER_RSC_ID_POOL;
import static com.linbit.linstor.numberpool.NumberPoolModule.MINOR_NUMBER_POOL;
import static com.linbit.linstor.numberpool.NumberPoolModule.SNAPSHOPT_SHIPPING_PORT_POOL;
import static com.linbit.linstor.numberpool.NumberPoolModule.SPECIAL_SATELLTE_PORT_POOL;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class SatelliteNumberPoolModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(DynamicNumberPool.class)
            .annotatedWith(Names.named(MINOR_NUMBER_POOL))
            .to(SatelliteDynamicNumberPool.class);
        bind(DynamicNumberPool.class)
            .annotatedWith(Names.named(SPECIAL_SATELLTE_PORT_POOL))
            .to(SatelliteDynamicNumberPool.class);
        bind(DynamicNumberPool.class)
            .annotatedWith(Names.named(LAYER_RSC_ID_POOL))
            .to(SatelliteDynamicNumberPool.class);
        bind(DynamicNumberPool.class)
            .annotatedWith(Names.named(SNAPSHOPT_SHIPPING_PORT_POOL))
            .to(SatelliteDynamicNumberPool.class);
    }
}
