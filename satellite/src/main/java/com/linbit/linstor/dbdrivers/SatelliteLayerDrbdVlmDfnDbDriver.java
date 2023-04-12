package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmDfnData;

import javax.inject.Inject;

public class SatelliteLayerDrbdVlmDfnDbDriver implements LayerDrbdVlmDfnDatabaseDriver
{
    @Inject
    public SatelliteLayerDrbdVlmDfnDbDriver()
    {
    }

    @Override
    public void create(DrbdVlmDfnData<?> drbdVlmDfnDataRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(DrbdVlmDfnData<?> drbdVlmDfnDataRef) throws DatabaseException
    {
        // no-op
    }
}

