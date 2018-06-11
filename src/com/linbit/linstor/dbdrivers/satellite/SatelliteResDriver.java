package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.linstor.ResourceData;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import javax.inject.Inject;

public class SatelliteResDriver implements ResourceDataDatabaseDriver
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
    public StateFlagsPersistence<ResourceData> getStateFlagPersistence()
    {
        return (StateFlagsPersistence<ResourceData>) stateFlagsDriver;
    }

    @Override
    public void create(ResourceData res)
    {
        // no-op
    }

    @Override
    public void delete(ResourceData resourceData)
    {
        // no-op
    }
}
