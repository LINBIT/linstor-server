package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

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
