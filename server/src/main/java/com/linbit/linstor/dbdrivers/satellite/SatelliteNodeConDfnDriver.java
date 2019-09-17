package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import javax.inject.Inject;

public class SatelliteNodeConDfnDriver implements NodeConnectionDatabaseDriver
{
    private final AccessContext dbCtx;

    @Inject
    public SatelliteNodeConDfnDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @Override
    public void create(NodeConnection nodeConDfnData)
    {
        // no-op
    }

    @Override
    public void delete(NodeConnection nodeConDfnData)
    {
        // no-op
    }
}
