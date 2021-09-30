package com.linbit.linstor.transaction;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;

public class ControllerK8sCrdCurrentTransactionMgr extends BaseControllerK8sCrdTransactionMgr<GenCrdCurrent.Rollback>
{
    public ControllerK8sCrdCurrentTransactionMgr(ControllerK8sCrdDatabase controllerK8sCrdDatabaseRef)
    {
        super(
            controllerK8sCrdDatabaseRef,
            new BaseControllerK8sCrdTransactionMgrContext<>(
                GenCrdCurrent::databaseTableToCustomResourceClass,
                GenCrdCurrent.Rollback.class,
                GenCrdCurrent::newRollbackCrd,
                GenCrdCurrent::specToCrd
            )
        );
    }
}
