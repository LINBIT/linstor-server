package com.linbit.linstor.dbdrivers;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteVolDriver implements VolumeDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteVolDriver()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<Volume> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<Volume>) stateFlagsDriver;
    }

    @Override
    public void create(Volume vol)
    {
        // no-op
    }

    @Override
    public void delete(Volume vlm)
    {
        // no-op
    }
}
