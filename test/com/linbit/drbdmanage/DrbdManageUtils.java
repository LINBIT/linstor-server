package com.linbit.drbdmanage;

final class DrbdManageUtils
{
    static void clearCaches()
    {
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
