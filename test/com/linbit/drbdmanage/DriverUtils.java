package com.linbit.drbdmanage;

import com.linbit.drbdmanage.dbdrivers.DatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.NoOpDriver;
import com.linbit.drbdmanage.security.DbAccessor;
import com.linbit.drbdmanage.security.NoOpSecurityDriver;

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

    public static void satelliteMode()
    {
        DrbdManage.securityDbDriver = new NoOpSecurityDriver();
        DrbdManage.persistenceDbDriver = new NoOpDriver();
    }
}
