package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

import java.util.Date;

public class SatelliteResDriver implements ResourceDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();
    private final AccessContext dbCtx;

    @Inject
    public SatelliteResDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<AbsResource<Resource>> getStateFlagPersistence()
    {
        return (StateFlagsPersistence<AbsResource<Resource>>) stateFlagsDriver;
    }

    @Override
    public void create(AbsResource<Resource> res)
    {
        // no-op
    }

    @Override
    public void delete(AbsResource<Resource> resource)
    {
        // no-op
    }

    @Override
    public SingleColumnDatabaseDriver<AbsResource<Resource>, Date> getCreateTimeDriver()
    {
        return (SingleColumnDatabaseDriver<AbsResource<Resource>, Date>) noopSingleColDriver;
    }
}
