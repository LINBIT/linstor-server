package com.linbit.drbdmanage.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.drbdmanage.NetInterfaceDataDatabaseDriver;
import com.linbit.drbdmanage.NetInterfaceName;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.StorPoolDefinition;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;

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
    public NodeDataDatabaseDriver getNodeDatabaseDriver()
    {
        return null; // intentionally null
    }

    @Override
    public ResourceDataDatabaseDriver getResourceDataDatabaseDriver(NodeName nodeName, ResourceName resName)
    {
        return null; // intentionally null
    }

    @Override
    public ResourceDefinitionDataDatabaseDriver getResourceDefinitionDataDatabaseDriver(ResourceName resName)
    {
        return null; // intentionally null
    }

    @Override
    public VolumeDataDatabaseDriver getVolumeDataDatabaseDriver()
    {
        return null; // intentionally null
    }

    @Override
    public VolumeDefinitionDataDatabaseDriver getVolumeDefinitionDataDatabaseDriver(ResourceDefinition resDfn, VolumeNumber volNr)
    {
        return null; // intentionally null
    }

    @Override
    public StorPoolDefinitionDataDatabaseDriver getStorPoolDefinitionDataDatabaseDriver(StorPoolName name)
    {
        return null; // intentionally null
    }

    @Override
    public StorPoolDataDatabaseDriver getStorPoolDataDatabaseDriver(Node nodeRef, StorPoolDefinition storPoolDfnRef)
    {
        return null; // intentionally null
    }

    @Override
    public NetInterfaceDataDatabaseDriver getNetInterfaceDataDatabaseDriver(Node node, NetInterfaceName name)
    {
        return null; // intentionally null
    }

}
