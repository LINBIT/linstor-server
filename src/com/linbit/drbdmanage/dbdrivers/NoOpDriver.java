package com.linbit.drbdmanage.dbdrivers;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.MapDatabaseDriver;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.ServiceName;
import com.linbit.drbdmanage.NetInterface;
import com.linbit.drbdmanage.NetInterfaceName;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.StorPool;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

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
        return null;
    }

    @Override
    public PropsConDatabaseDriver getPropsDatabaseDriver(Connection dbCon, String instanceName)
    {
        return new PropsConDatabaseDriver()
        {

            @Override
            public void removeAll(Connection con) throws SQLException
            {
            }

            @Override
            public void remove(Connection con, Set<String> keys) throws SQLException
            {
            }

            @Override
            public void remove(Connection con, String key) throws SQLException
            {
            }

            @Override
            public void persist(Connection con, Map<String, String> props) throws SQLException
            {
            }

            @Override
            public void persist(Connection con, String key, String value) throws SQLException
            {
            }

            @Override
            public Map<String, String> load(Connection con) throws SQLException
            {
                return new HashMap<>();
            }

            @Override
            public String getInstanceName()
            {
                return null;
            }
        };
    }

    @Override
    public NodeDatabaseDriver getNodeDatabaseDriver(String nodeName)
    {
        return new NodeDatabaseDriver()
        {

            @Override
            public StateFlagsPersistence getStateFlagPersistence()
            {
             // no-op
                return null;
            }

            @Override
            public ObjectDatabaseDriver<NodeType> getNodeTypeDriver()
            {
                // no-op
                return null;
            }

            @Override
            public void setConnection(Connection dbCon)
            {
                // no-op
            }

            @Override
            public ObjectDatabaseDriver<InetAddress> getNodeNetInterfaceDriver(NetInterfaceName netInterfaceName)
            {
                return null;
            }

            @Override
            public MapDatabaseDriver<ResourceName, Resource> getNodeResourceMapDriver()
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public MapDatabaseDriver<NetInterfaceName, NetInterface> getNodeNetInterfaceMapDriver()
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public MapDatabaseDriver<StorPoolName, StorPool> getNodeStorPoolMapDriver()
            {
                // TODO Auto-generated method stub
                return null;
            }
        };
    }

    @Override
    public ResourceDatabaseDriver getResourceDatabaseDriver(Resource res)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResourceDefinitionDatabaseDriver getResourceDefinitionDatabaseDriver(ResourceDefinition resDfn)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VolumeDatabaseDriver getVolumeDatabaseDriver(Volume volume)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VolumeDefinitionDatabaseDriver getVolumeDefinitionDatabaseDriver(VolumeDefinition volumeDefinition)
    {
        // TODO Auto-generated method stub
        return null;
    }
}
