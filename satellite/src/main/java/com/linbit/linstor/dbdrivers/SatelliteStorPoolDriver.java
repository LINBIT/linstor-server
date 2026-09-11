package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDatabaseDriver;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class SatelliteStorPoolDriver
    extends AbsSatelliteDbDriver<StorPool>
    implements StorPoolDatabaseDriver
{
    @Inject
    public SatelliteStorPoolDriver()
    {
        // no-op
    }
}
