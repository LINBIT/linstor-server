package com.linbit.drbdmanage.dbdrivers;

import java.sql.SQLException;

import com.linbit.ServiceName;
import com.linbit.drbdmanage.NetInterfaceDataDatabaseDriver;
import com.linbit.drbdmanage.NetInterfaceName;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.StorPoolDefinition;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
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
    PropsConDatabaseDriver getPropsDatabaseDriver(String instanceName);

    /**
     * Returns the database driver specific implementation for {@link NodeData}-IO
     */
    NodeDataDatabaseDriver getNodeDatabaseDriver();

    /**
     * Returns the database driver specific implementation for {@link ResourceData}-IO
     */
    ResourceDataDatabaseDriver getResourceDataDatabaseDriver(NodeName nodeName, ResourceName resName);

    /**
     * Returns the database driver specific implementation for {@link ResourceDefinitionData}-IO
     */
    ResourceDefinitionDataDatabaseDriver getResourceDefinitionDataDatabaseDriver(ResourceName resName);

    /**
     * Returns the database driver specific implementation for {@link VolumeData}-IO
     */
    VolumeDataDatabaseDriver getVolumeDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for {@link VolumeDefinitionData}-IO
     */
    VolumeDefinitionDataDatabaseDriver getVolumeDefinitionDataDatabaseDriver(VolumeDefinitionData volumeDefinition);

    /**
     * Returns the database driver specific implementation for {@link StorPoolDefinitionData}-IO
     */
    StorPoolDefinitionDataDatabaseDriver getStorPoolDefinitionDataDatabaseDriver(StorPoolName name);

    /**
     * Returns the database driver specific implementation for {@link StorPoolData}-IO
     */
    StorPoolDataDatabaseDriver getStorPoolDataDatabaseDriver(Node nodeRef, StorPoolDefinition storPoolDfnRef);

    /**
     * Returns the database driver specific implementation for {@link NetInterfaceDataDatabaseDriver}-IO
     */
    NetInterfaceDataDatabaseDriver getNetInterfaceDataDatabaseDriver(Node node, NetInterfaceName name);

}
