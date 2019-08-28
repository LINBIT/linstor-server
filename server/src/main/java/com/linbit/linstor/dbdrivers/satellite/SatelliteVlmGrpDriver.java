package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.linstor.core.objects.VolumeGroupData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDataDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteVlmGrpDriver implements VolumeGroupDataDatabaseDriver
{
    @Inject
    public SatelliteVlmGrpDriver()
    {
    }

    @Override
    public void create(VolumeGroupData vlmGrpDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(VolumeGroupData vlmGrpDataRef) throws DatabaseException
    {
        // no-op
    }
}
