package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.VolumeDefinitionData;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import javax.inject.Inject;

public class SatelliteVolDfnDriver implements VolumeDefinitionDataDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteVolDfnDriver()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<VolumeDefinitionData> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<VolumeDefinitionData>) stateFlagsDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<VolumeDefinitionData, Long> getVolumeSizeDriver()
    {
        return (SingleColumnDatabaseDriver<VolumeDefinitionData, Long>) singleColDriver;
    }

    @Override
    public void create(VolumeDefinitionData volDfnData)
    {
        // no-op
    }

    @Override
    public void delete(VolumeDefinitionData data)
    {
        // no-op
    }
}
