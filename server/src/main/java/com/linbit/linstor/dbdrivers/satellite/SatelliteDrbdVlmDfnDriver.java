package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.dbdrivers.interfaces.DrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.storage.layer.data.DrbdVlmDfnData;

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
    public SingleColumnDatabaseDriver<DrbdVlmDfnData, MinorNumber> getMinorDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdVlmDfnData, MinorNumber>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<DrbdVlmDfnData, Integer> getPeerSlotsDriver()
    {
        return (SingleColumnDatabaseDriver<DrbdVlmDfnData, Integer>) singleColDriver;
    }

}
