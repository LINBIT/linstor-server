package com.linbit.drbdmanage.dbdrivers;

import java.sql.SQLException;

import com.linbit.ServiceName;
import com.linbit.drbdmanage.ConnectionDefinitionData;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.dbdrivers.interfaces.ConnectionDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
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
    PropsConDatabaseDriver getPropsDatabaseDriver();

    /**
     * Returns the database driver specific implementation for {@link NodeData}-IO
     * @param nodeName
     */
    NodeDataDatabaseDriver getNodeDatabaseDriver();

    /**
     * Returns the database driver specific implementation for {@link ResourceData}-IO
     */
    ResourceDataDatabaseDriver getResourceDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for {@link ResourceDefinitionData}-IO
     */
    ResourceDefinitionDataDatabaseDriver getResourceDefinitionDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for {@link VolumeData}-IO
     */
    VolumeDataDatabaseDriver getVolumeDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for {@link VolumeDefinitionData}-IO
     */
    VolumeDefinitionDataDatabaseDriver getVolumeDefinitionDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for {@link StorPoolDefinitionData}-IO
     */
    StorPoolDefinitionDataDatabaseDriver getStorPoolDefinitionDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for {@link StorPoolData}-IO
     */
    StorPoolDataDatabaseDriver getStorPoolDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for {@link NetInterfaceData}-IO
     */
    NetInterfaceDataDatabaseDriver getNetInterfaceDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for {@link ConnectionDefinitionData}-IO
     */
    ConnectionDefinitionDataDatabaseDriver getConnectionDefinitionDatabaseDriver();
}
