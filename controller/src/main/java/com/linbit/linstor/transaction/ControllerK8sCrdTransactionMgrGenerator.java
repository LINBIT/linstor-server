package com.linbit.linstor.transaction;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;

import jakarta.inject.Inject;

import com.google.inject.Provider;

public class ControllerK8sCrdTransactionMgrGenerator implements TransactionMgrGenerator
{
    private final Provider<ControllerK8sCrdDatabase> controllerDatabase;

    @Inject
    public ControllerK8sCrdTransactionMgrGenerator(
        Provider<ControllerK8sCrdDatabase> controllerDatabaseRef
    )
    {
        controllerDatabase = controllerDatabaseRef;
    }

    @Override
    public ControllerK8sCrdTransactionMgr startTransaction()
    {
        return new ControllerK8sCrdTransactionMgr(controllerDatabase.get());
    }
}
