package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdVlmDatabaseDriver;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdVlmObject;

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
    public SingleColumnDatabaseDriver<DrbdVlmObject, String> getMetaDiskDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdVlmObject, String>) singleColDriver;
    }
}
