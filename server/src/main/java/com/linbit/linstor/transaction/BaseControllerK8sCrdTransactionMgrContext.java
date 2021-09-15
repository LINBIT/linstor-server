package com.linbit.linstor.transaction;

import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.RollbackCrd;

import java.util.function.Function;
import java.util.function.Supplier;

public class BaseControllerK8sCrdTransactionMgrContext<ROLLBACK_CRD extends RollbackCrd>
{
    private final Function<DatabaseTable, Class<? extends LinstorCrd<? extends LinstorSpec>>> dbTableToCrdClass;
    private final Class<ROLLBACK_CRD> rollbackClass;
    private final Supplier<ROLLBACK_CRD> rollbackCrdSupplier;
    private final Function<LinstorSpec, LinstorCrd<LinstorSpec>> specToCrd;

    public BaseControllerK8sCrdTransactionMgrContext(
        Function<DatabaseTable, Class<? extends LinstorCrd<? extends LinstorSpec>>> dbTableToCrdClassRef,
        Class<ROLLBACK_CRD> rollbackClassRef,
        Supplier<ROLLBACK_CRD> rollbackCrdSupplierRef,
        Function<LinstorSpec, LinstorCrd<LinstorSpec>> specToCrdRef
    )
    {
        dbTableToCrdClass = dbTableToCrdClassRef;
        rollbackClass = rollbackClassRef;
        rollbackCrdSupplier = rollbackCrdSupplierRef;
        specToCrd = specToCrdRef;
    }

    public Function<DatabaseTable, Class<? extends LinstorCrd<? extends LinstorSpec>>> getDbTableToCrdClass()
    {
        return dbTableToCrdClass;
    }

    public Class<ROLLBACK_CRD> getRollbackClass()
    {
        return rollbackClass;
    }

    public Supplier<ROLLBACK_CRD> getRollbackCrdSupplier()
    {
        return rollbackCrdSupplier;
    }

    public Function<LinstorSpec, LinstorCrd<LinstorSpec>> getSpecToCrd()
    {
        return specToCrd;
    }
}
