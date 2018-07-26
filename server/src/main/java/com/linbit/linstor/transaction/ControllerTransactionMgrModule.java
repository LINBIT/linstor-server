package com.linbit.linstor.transaction;

import com.linbit.linstor.LinStorModule;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class ControllerTransactionMgrModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(TransactionMgr.class)
            .annotatedWith(Names.named(LinStorModule.TRANS_MGR_GENERATOR))
            .to(ControllerTransactionMgr.class);
    }
}
