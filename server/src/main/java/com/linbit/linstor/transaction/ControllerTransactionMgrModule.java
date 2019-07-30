package com.linbit.linstor.transaction;

import com.google.inject.AbstractModule;

public class ControllerTransactionMgrModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(TransactionMgrGenerator.class).to(ControllerSQLTransactionMgrGenerator.class);
    }
}
