package com.linbit.linstor.dbdrivers.satellite;

import javax.inject.Inject;

import com.linbit.linstor.ResourceConnectionData;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

public class SatelliteResConDfnDriver implements ResourceConnectionDataDatabaseDriver
{
    private final AccessContext dbCtx;
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();

    @Inject
    public SatelliteResConDfnDriver(
        @SystemContext AccessContext dbCtxRef
    )
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

    @Override
    public StateFlagsPersistence<ResourceConnectionData> getStateFlagPersistence()
    {
        return (StateFlagsPersistence<ResourceConnectionData>) stateFlagsDriver;
    }
}
