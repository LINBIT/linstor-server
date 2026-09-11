package com.linbit.linstor.transaction.manager;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

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
