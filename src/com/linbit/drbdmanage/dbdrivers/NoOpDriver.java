package com.linbit.drbdmanage.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDatabaseDriver;

public class NoOpDriver implements DatabaseDriver
{
    public static final ServiceName DFLT_SERVICE_INSTANCE_NAME;

    static
    {
        try
        {
            DFLT_SERVICE_INSTANCE_NAME = new ServiceName("EmptyDatabaseService");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The builtin default service instance name is not a valid ServiceName",
                nameExc
            );
        }
    }

    @Override
    public ServiceName getDefaultServiceInstanceName()
    {
        return DFLT_SERVICE_INSTANCE_NAME;
    }

    @Override
    public String getDefaultConnectionUrl()
    {
        return null; // intentionally null
    }

    @Override
    public PropsConDatabaseDriver getPropsDatabaseDriver(String instanceName)
    {
        return null; // intentionally null
    }

    @Override
    public NodeDatabaseDriver getNodeDatabaseDriver(String nodeName)
    {
        return null; // intentionally null
    }

    @Override
    public ResourceDatabaseDriver getResourceDatabaseDriver(Resource res)
    {
        return null; // intentionally null
    }

    @Override
    public ResourceDefinitionDatabaseDriver getResourceDefinitionDatabaseDriver(ResourceDefinition resDfn)
    {
        return null; // intentionally null
    }

    @Override
    public VolumeDatabaseDriver getVolumeDatabaseDriver(Volume volume)
    {
        return null; // intentionally null
    }

    @Override
    public VolumeDefinitionDatabaseDriver getVolumeDefinitionDatabaseDriver(VolumeDefinition volumeDefinition)
    {
        return null; // intentionally null
    }
}
