package com.linbit.linstor.transaction;

import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;

import javax.inject.Inject;

import com.google.inject.Provider;

public class ControllerETCDTransactionMgrGenerator implements TransactionMgrGenerator
{
    private final Provider<ControllerETCDDatabase> controllerDatabase;
    private final CtrlConfig ctrlCfg;

    @Inject
    public ControllerETCDTransactionMgrGenerator(
        Provider<ControllerETCDDatabase> controllerDatabaseRef,
        CtrlConfig ctrlCfgRef
    )
    {
        ctrlCfg = ctrlCfgRef;
        controllerDatabase = controllerDatabaseRef;
    }

    @Override
    public ControllerETCDTransactionMgr startTransaction()
    {
        return new ControllerETCDTransactionMgr(
            controllerDatabase.get(),
            ctrlCfg.getEtcdOperationsPerTransaction(),
            ctrlCfg.getEtcdPrefix()
        );
    }
}
