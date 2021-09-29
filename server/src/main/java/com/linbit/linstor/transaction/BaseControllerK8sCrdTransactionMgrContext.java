package com.linbit.linstor.transaction;

import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;

import java.util.function.Function;

public class BaseControllerK8sCrdTransactionMgrContext
{
    private final Function<DatabaseTable, Class<? extends LinstorCrd<? extends LinstorSpec>>> dbTableToCrdClass;
    private final Function<LinstorSpec, LinstorCrd<LinstorSpec>> specToCrd;

    public BaseControllerK8sCrdTransactionMgrContext(
        Function<DatabaseTable, Class<? extends LinstorCrd<? extends LinstorSpec>>> dbTableToCrdClassRef,
        Function<LinstorSpec, LinstorCrd<LinstorSpec>> specToCrdRef
    )
    {
        dbTableToCrdClass = dbTableToCrdClassRef;
        specToCrd = specToCrdRef;
    }

    public Function<DatabaseTable, Class<? extends LinstorCrd<? extends LinstorSpec>>> getDbTableToCrdClass()
    {
        return dbTableToCrdClass;
    }

    public Function<LinstorSpec, LinstorCrd<LinstorSpec>> getSpecToCrd()
    {
        return specToCrd;
    }
}
