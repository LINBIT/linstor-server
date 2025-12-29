package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.transaction.manager.ControllerSQLTransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;

import com.google.inject.AbstractModule;

public class ControllerTransactionMgrModule extends AbstractModule
{
    private final DatabaseDriverInfo.DatabaseType dbType;

    public ControllerTransactionMgrModule(DatabaseDriverInfo.DatabaseType dbTypeRef)
    {
        dbType = dbTypeRef;
    }

    @Override
    protected void configure()
    {
        switch (dbType)
        {
            case SQL:
                bind(TransactionMgrGenerator.class).to(ControllerSQLTransactionMgrGenerator.class);
                break;
            case K8S_CRD:
                bind(TransactionMgrGenerator.class).to(ControllerK8sCrdTransactionMgrGenerator.class);
                break;
            default:
                throw new ImplementationError("Unknown database type: " + dbType);
        }
    }
}
