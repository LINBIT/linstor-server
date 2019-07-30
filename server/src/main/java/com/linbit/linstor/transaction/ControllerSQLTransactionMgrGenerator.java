package com.linbit.linstor.transaction;

import com.linbit.linstor.ControllerDatabase;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.SQLException;

@Singleton
public class ControllerSQLTransactionMgrGenerator implements TransactionMgrGenerator
{
    private final ControllerDatabase controllerDatabase;

    @Inject
    public ControllerSQLTransactionMgrGenerator(ControllerDatabase controllerDatabaseRef)
    {
        controllerDatabase = controllerDatabaseRef;
    }

    @Override
    public TransactionMgr startTransaction()
    {
        ControllerTransactionMgr controllerTransactionMgr;
        try
        {
            controllerTransactionMgr = new ControllerTransactionMgr(controllerDatabase);
        }
        catch (SQLException sqlExc)
        {
            throw new TransactionException("Failed to start transaction", sqlExc);
        }
        return controllerTransactionMgr;
    }
}
