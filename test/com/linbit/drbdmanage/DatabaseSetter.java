package com.linbit.drbdmanage;

import com.linbit.drbdmanage.dbdrivers.DatabaseDriver;
import com.linbit.drbdmanage.security.DbAccessor;

public class DatabaseSetter
{
    public static void setDatabaseClasses(DbAccessor secureDbDriver, DatabaseDriver persistenceDbDriver)
    {
        DrbdManage.securityDbDriver = secureDbDriver;
        DrbdManage.persistenceDbDriver = persistenceDbDriver;
    }
}
