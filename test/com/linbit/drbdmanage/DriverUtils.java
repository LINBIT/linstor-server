package com.linbit.drbdmanage;

import com.linbit.drbdmanage.dbdrivers.DatabaseDriver;
import com.linbit.drbdmanage.security.DbAccessor;

public class DriverUtils
{
    public static void setDatabaseClasses(DbAccessor secureDbDriver, DatabaseDriver persistenceDbDriver)
    {
        DrbdManage.securityDbDriver = secureDbDriver;
        DrbdManage.persistenceDbDriver = persistenceDbDriver;
    }
    
    public static void clearCaches()
    {
        ConnectionDefinitionDataDerbyDriver.clearCache();
        NetInterfaceDataDerbyDriver.clearCache();
        NodeDataDerbyDriver.clearCache();
        ResourceDataDerbyDriver.clearCache();
        ResourceDefinitionDataDerbyDriver.clearCache();
        StorPoolDataDerbyDriver.clearCache();
        StorPoolDefinitionDataDerbyDriver.clearCache();
        VolumeDataDerbyDriver.clearCache();
        VolumeDefinitionDataDerbyDriver.clearCache();
    }
}
