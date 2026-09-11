package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class SatelliteLayerDrbdVlmDfnDbDriver
    extends AbsSatelliteDbDriver<DrbdVlmDfnData<?>>
    implements LayerDrbdVlmDfnDatabaseDriver
{
    @Inject
    public SatelliteLayerDrbdVlmDfnDbDriver()
    {
    }
}
