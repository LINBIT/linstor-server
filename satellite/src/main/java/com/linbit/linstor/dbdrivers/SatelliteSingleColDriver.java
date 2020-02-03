package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

public class SatelliteSingleColDriver<NOOP_KEY, NOOP> implements SingleColumnDatabaseDriver<NOOP_KEY, NOOP>
{
    @Override
    public void update(NOOP_KEY parent, NOOP element)
    {
        // no-op
    }
}
