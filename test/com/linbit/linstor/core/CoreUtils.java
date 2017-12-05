package com.linbit.linstor.core;

import java.util.Map;

import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SatelliteDbDriver;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.dbdrivers.DatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.DbAccessor;
import com.linbit.linstor.security.NoOpSecurityDriver;

public class CoreUtils
{
    public static void setDatabaseClasses(DbAccessor secureDbDriver, DatabaseDriver persistenceDbDriver)
    {
        LinStor.securityDbDriver = secureDbDriver;
        LinStor.persistenceDbDriver = persistenceDbDriver;
    }

    public static void satelliteMode(
        AccessContext accCtx,
        Map<NodeName, Node> nodesMap,
        Map<ResourceName, ResourceDefinition> resDfnMap,
        Map<StorPoolName, StorPoolDefinition> storPoolDfnMap
    )
    {
        LinStor.securityDbDriver = new NoOpSecurityDriver(accCtx);
        LinStor.persistenceDbDriver = new SatelliteDbDriver(
            accCtx,
            nodesMap,
            resDfnMap,
            storPoolDfnMap
        );
    }

    private CoreUtils()
    {
    }
}
