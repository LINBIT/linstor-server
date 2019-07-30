package com.linbit.linstor.transaction;

import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;

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
            case ECTD:
                throw new RuntimeException("ECTD TransactionMgr not implemented.");
            default:
        }
    }
}
