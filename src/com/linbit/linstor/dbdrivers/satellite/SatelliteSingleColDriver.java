package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;

public class SatelliteSingleColDriver<NOOP_KEY, NOOP> implements SingleColumnDatabaseDriver<NOOP_KEY, NOOP>
{
    @Override
    public void update(NOOP_KEY parent, NOOP element)
    {
        // no-op
    }
}
