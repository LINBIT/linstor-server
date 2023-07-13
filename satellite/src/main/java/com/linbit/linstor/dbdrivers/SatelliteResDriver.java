package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Date;

@Singleton
public class SatelliteResDriver
    extends AbsSatelliteDbDriver<AbsResource<Resource>>
    implements ResourceDatabaseDriver
{
    private final StateFlagsPersistence<AbsResource<Resource>> stateFlagsDriver;
    private final SingleColumnDatabaseDriver<AbsResource<Resource>, Date> createTimeDriver;

    @Inject
    public SatelliteResDriver()
    {
        stateFlagsDriver = getNoopFlagDriver();
        createTimeDriver = getNoopColumnDriver();
    }

    @Override
    public StateFlagsPersistence<AbsResource<Resource>> getStateFlagPersistence()
    {
        return stateFlagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<AbsResource<Resource>, Date> getCreateTimeDriver()
    {
        return createTimeDriver;
    }
}
