package com.linbit.linstor.transaction.manager;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerSQLDatabase;
import com.linbit.linstor.transaction.TransactionException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.SQLException;

@Singleton
public class ControllerSQLTransactionMgrGenerator implements TransactionMgrGenerator
{
    private final ControllerSQLDatabase controllerDatabase;

    @Inject
    public ControllerSQLTransactionMgrGenerator(ControllerDatabase controllerDatabaseRef)
    {
        controllerDatabase = (ControllerSQLDatabase) controllerDatabaseRef;
    }

    @Override
    public TransactionMgr startTransaction()
    {
        ControllerSQLTransactionMgr controllerSQLTransactionMgr;
        try
        {
            controllerSQLTransactionMgr = new ControllerSQLTransactionMgr(controllerDatabase);
        }
        catch (SQLException sqlExc)
        {
            throw new TransactionException("Failed to start transaction", sqlExc);
        }
        return controllerSQLTransactionMgr;
    }
}
