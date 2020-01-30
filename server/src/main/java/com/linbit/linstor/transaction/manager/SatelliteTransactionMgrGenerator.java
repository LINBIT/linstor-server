package com.linbit.linstor.transaction.manager;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteTransactionMgrGenerator implements TransactionMgrGenerator
{
    @Inject
    public SatelliteTransactionMgrGenerator()
    {
    }

    @Override
    public TransactionMgr startTransaction()
    {
        return new SatelliteTransactionMgr();
    }
}
