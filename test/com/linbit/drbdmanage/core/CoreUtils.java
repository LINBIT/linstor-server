package com.linbit.drbdmanage.core;

import java.util.Map;

import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.SatelliteDbDriver;
import com.linbit.drbdmanage.StorPoolDefinition;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.dbdrivers.DatabaseDriver;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.DbAccessor;
import com.linbit.drbdmanage.security.NoOpSecurityDriver;

public class CoreUtils
{
    public static void setDatabaseClasses(DbAccessor secureDbDriver, DatabaseDriver persistenceDbDriver)
    {
        DrbdManage.securityDbDriver = secureDbDriver;
        DrbdManage.persistenceDbDriver = persistenceDbDriver;
    }

    public static void satelliteMode(
        AccessContext accCtx,
        Map<NodeName, Node> nodesMap,
        Map<ResourceName, ResourceDefinition> resDfnMap,
        Map<StorPoolName, StorPoolDefinition> storPoolDfnMap
    )
    {
        DrbdManage.securityDbDriver = new NoOpSecurityDriver(accCtx);
        DrbdManage.persistenceDbDriver = new SatelliteDbDriver(
            accCtx,
            nodesMap,
            resDfnMap,
            storPoolDfnMap
        );
    }
}
