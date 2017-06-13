package com.linbit.drbdmanage.dbdrivers.derby;

import java.sql.Connection;
import java.util.Map;

import com.linbit.MapDatabaseDriver;
import com.linbit.drbdmanage.ConnectionDefinition;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public class ResourceDefinitionDerbyDriver implements ResourceDefinitionDatabaseDriver
{

    public ResourceDefinitionDerbyDriver(ResourceDefinition resDfn)
    {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void setConnection(Connection dbCon)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public MapDatabaseDriver<NodeName, Map<Integer, ConnectionDefinition>> getConnectionMapDriver()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MapDatabaseDriver<VolumeNumber, VolumeDefinition> getVolumeMapDriver()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MapDatabaseDriver<NodeName, Resource> getResourceMapDriver()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StateFlagsPersistence getStateFlagsPersistence()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
