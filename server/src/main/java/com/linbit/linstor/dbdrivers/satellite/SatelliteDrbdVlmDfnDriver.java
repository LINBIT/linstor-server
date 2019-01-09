package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.dbdrivers.interfaces.DrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdVlmDfnObject;

import javax.inject.Inject;

public class SatelliteDrbdVlmDfnDriver implements DrbdVlmDfnDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteDrbdVlmDfnDriver()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdVlmDfnObject, MinorNumber> getMinorDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdVlmDfnObject, MinorNumber>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdVlmDfnObject, Integer> getPeerSlotsDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdVlmDfnObject, Integer>) singleColDriver;
    }

}
