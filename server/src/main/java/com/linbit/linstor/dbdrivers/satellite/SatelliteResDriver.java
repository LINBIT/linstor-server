package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteResDriver implements ResourceDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final AccessContext dbCtx;

    @Inject
    public SatelliteResDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<Resource> getStateFlagPersistence()
    {
        return (StateFlagsPersistence<Resource>) stateFlagsDriver;
    }

    @Override
    public void create(Resource res)
    {
        // no-op
    }

    @Override
    public void delete(Resource resource)
    {
        // no-op
    }
}
