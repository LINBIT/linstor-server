package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.NodeConnectionData;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import javax.inject.Inject;

public class SatelliteNodeConDfnDriver implements NodeConnectionDataDatabaseDriver
{
    private final AccessContext dbCtx;

    @Inject
    public SatelliteNodeConDfnDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @Override
    public void create(NodeConnectionData nodeConDfnData)
    {
        // no-op
    }

    @Override
    public void delete(NodeConnectionData nodeConDfnData)
    {
        // no-op
    }
}
