package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.VolumeConnection;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteVolConDfnDriver
    extends AbsSatelliteDbDriver<VolumeConnection>
    implements VolumeConnectionDatabaseDriver
{
    @Inject
    public SatelliteVolConDfnDriver()
    {
        // no-op
    }
}
