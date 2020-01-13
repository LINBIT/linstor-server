package com.linbit.linstor.transaction;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.transaction.TransactionMgrGenerator;

import javax.inject.Inject;

public class ControllerETCDTransactionMgrGenerator implements TransactionMgrGenerator
{
    private final ControllerETCDDatabase controllerDatabase;

    @Inject
    public ControllerETCDTransactionMgrGenerator(ControllerDatabase controllerDatabaseRef)
    {
        controllerDatabase = (ControllerETCDDatabase) controllerDatabaseRef;
    }

    @Override
    public ControllerETCDTransactionMgr startTransaction()
    {
        return new ControllerETCDTransactionMgr(controllerDatabase);
    }
}
