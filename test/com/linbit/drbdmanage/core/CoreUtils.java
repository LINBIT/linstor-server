package com.linbit.drbdmanage.core;

import com.linbit.drbdmanage.dbdrivers.DatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.NoOpDriver;
import com.linbit.drbdmanage.security.DbAccessor;
import com.linbit.drbdmanage.security.NoOpSecurityDriver;

public class CoreUtils
{
    public static void setDatabaseClasses(DbAccessor secureDbDriver, DatabaseDriver persistenceDbDriver)
    {
        DrbdManage.securityDbDriver = secureDbDriver;
        DrbdManage.persistenceDbDriver = persistenceDbDriver;
    }

    public static void satelliteMode()
    {
        DrbdManage.securityDbDriver = new NoOpSecurityDriver();
        DrbdManage.persistenceDbDriver = new NoOpDriver();
    }
}
