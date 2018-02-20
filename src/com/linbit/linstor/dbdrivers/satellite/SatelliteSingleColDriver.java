package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;

public class SatelliteSingleColDriver<NOOP_KEY, NOOP> implements SingleColumnDatabaseDriver<NOOP_KEY, NOOP>
{
    @Override
    public void update(NOOP_KEY parent, NOOP element, TransactionMgr transMgr)
    {
        // no-op
    }
}
