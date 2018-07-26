package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.linstor.ResourceConnectionData;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import javax.inject.Inject;

public class SatelliteResConDfnDriver implements ResourceConnectionDataDatabaseDriver
{
    private final AccessContext dbCtx;

    @Inject
    public SatelliteResConDfnDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @Override
    public void create(ResourceConnectionData conDfnData)
    {
        // no-op
    }

    @Override
    public void delete(ResourceConnectionData data)
    {
        // no-op
    }
}
