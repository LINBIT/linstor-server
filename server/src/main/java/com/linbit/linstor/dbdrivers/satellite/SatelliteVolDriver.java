package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.VolumeData;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import javax.inject.Inject;

public class SatelliteVolDriver implements VolumeDataDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteVolDriver()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<VolumeData> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<VolumeData>) stateFlagsDriver;
    }

    @Override
    public void create(VolumeData vol)
    {
        // no-op
    }

    @Override
    public void delete(VolumeData data)
    {
        // no-op
    }
}
