package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteNodeConDfnDriver
    extends AbsSatelliteDbDriver<NodeConnection>
    implements NodeConnectionDatabaseDriver
{
    @Inject
    public SatelliteNodeConDfnDriver()
    {
    }
}
