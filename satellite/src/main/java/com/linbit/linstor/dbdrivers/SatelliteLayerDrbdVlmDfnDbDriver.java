package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;

import javax.inject.Inject;
import javax.inject.Singleton;

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
