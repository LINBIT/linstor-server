package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdVlmDatabaseDriver;
import com.linbit.linstor.storage2.layer.data.DrbdVlmData;

import javax.inject.Inject;

public class SatelliteDrbdVlmDriver implements DrbdVlmDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteDrbdVlmDriver()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdVlmData, String> getMetaDiskDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdVlmData, String>) singleColDriver;
    }
}
