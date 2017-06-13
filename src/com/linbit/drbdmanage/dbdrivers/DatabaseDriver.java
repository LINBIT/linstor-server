package com.linbit.drbdmanage.dbdrivers;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.ServiceName;
import com.linbit.drbdmanage.NodeData;
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
import com.linbit.drbdmanage.propscon.PropsContainer;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface DatabaseDriver
{
    /**
     * Returns the default service name for a database service instance using this driver
     */
    ServiceName getDefaultServiceInstanceName();

    /**
     * Returns the default connection URL for the driver's database type
     *
     * @return Default URL for connecting to the database
     */
    String getDefaultConnectionUrl();

    /**
     * Returns the database driver specific implementation for {@link PropsContainer}-IO.
     * @throws SQLException
     */
    PropsConDatabaseDriver getPropsDatabaseDriver(Connection con, String instanceName) throws SQLException;

    /**
     * Returns the database driver specific implementation for {@link NodeData}-IO
     */
    NodeDatabaseDriver getNodeDatabaseDriver(String nodeName);

    /**
     * Returns the database driver specific implementation for {@link Resource}-IO
     */
    ResourceDatabaseDriver getResourceDatabaseDriver(Resource res);

    /**
     * Returns the database driver specific implementation for {@link ResourceDefinition}-IO
     */
    ResourceDefinitionDatabaseDriver getResourceDefinitionDatabaseDriver(ResourceDefinition resDfn);

    /**
     * Returns the database driver specific implementation for {@link Volume}-IO
     */
    VolumeDatabaseDriver getVolumeDatabaseDriver(Volume volume);

    /**
     * Returns the database driver specific implementation for {@link VolumeDefinition}-IO
     */
    VolumeDefinitionDatabaseDriver getVolumeDefinitionDatabaseDriver(VolumeDefinition volumeDefinition);
}
