package com.linbit.linstor.transaction;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;

import javax.inject.Inject;

import com.google.inject.Provider;

public class ControllerK8sCrdTransactionMgrGenerator implements TransactionMgrGenerator
{
    private final Provider<ControllerK8sCrdDatabase> controllerDatabase;
    private final CtrlConfig ctrlCfg;

    @Inject
    public ControllerK8sCrdTransactionMgrGenerator(
        Provider<ControllerK8sCrdDatabase> controllerDatabaseRef,
        CtrlConfig ctrlCfgRef
    )
    {
        ctrlCfg = ctrlCfgRef;
        controllerDatabase = controllerDatabaseRef;
    }

    @Override
    public ControllerK8sCrdTransactionMgr startTransaction()
    {
        return new ControllerK8sCrdTransactionMgr(controllerDatabase.get());
    }
}
