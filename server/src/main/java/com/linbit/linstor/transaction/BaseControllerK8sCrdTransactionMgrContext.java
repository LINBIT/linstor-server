package com.linbit.linstor.transaction;

import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;

import java.util.function.Function;

public class BaseControllerK8sCrdTransactionMgrContext
{
    private final Function<DatabaseTable, Class<? extends LinstorCrd<? extends LinstorSpec<?, ?>>>> dbTableToCrdClass;
    private final DatabaseTable[] allDbTables;
    private final String crdVersion;

    public BaseControllerK8sCrdTransactionMgrContext(
        Function<DatabaseTable, Class<? extends LinstorCrd<? extends LinstorSpec<?, ?>>>> dbTableToCrdClassRef,
        DatabaseTable[] allDbTablesRef,
        String crdVersionRef
    )
    {
        dbTableToCrdClass = dbTableToCrdClassRef;
        allDbTables = allDbTablesRef;
        crdVersion = crdVersionRef;
    }

    public Function<DatabaseTable, Class<? extends LinstorCrd<? extends LinstorSpec<?, ?>>>> getDbTableToCrdClass()
    {
        return dbTableToCrdClass;
    }

    public DatabaseTable[] getAllDatabaseTables()
    {
        return allDbTables;
    }

    public String getCrdVersion()
    {
        return crdVersion;
    }
}
