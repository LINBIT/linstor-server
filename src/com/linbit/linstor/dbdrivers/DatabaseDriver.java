package com.linbit.linstor.dbdrivers;

import java.sql.SQLException;
import com.linbit.ServiceName;
import com.linbit.TransactionMgr;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;

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
     * Fetches all {@link com.linbit.linstor.Node}s, {@link ResourceDefinition}s and {@link com.linbit.linstor.StorPoolDefinition}s from
     * the database.
     *
     * @param transMgr
     *
     * @throws SQLException
     */
    void loadAll(TransactionMgr transMgr) throws SQLException;

    /**
     * Returns the default connection URL for the driver's database type
     *
     * @return Default URL for connecting to the database
     */
    String getDefaultConnectionUrl();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.linstor.propscon.PropsContainer}-IO.
     */
    PropsConDatabaseDriver getPropsDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.linstor.NodeData}-IO
     */
    NodeDataDatabaseDriver getNodeDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.linstor.ResourceData}-IO
     */
    ResourceDataDatabaseDriver getResourceDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.linstor.ResourceDefinitionData}-IO
     */
    ResourceDefinitionDataDatabaseDriver getResourceDefinitionDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.linstor.VolumeData}-IO
     */
    VolumeDataDatabaseDriver getVolumeDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.linstor.VolumeDefinitionData}-IO
     */
    VolumeDefinitionDataDatabaseDriver getVolumeDefinitionDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.linstor.StorPoolDefinitionData}-IO
     */
    StorPoolDefinitionDataDatabaseDriver getStorPoolDefinitionDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.linstor.StorPoolData}-IO
     */
    StorPoolDataDatabaseDriver getStorPoolDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.linstor.NetInterfaceData}-IO
     */
    NetInterfaceDataDatabaseDriver getNetInterfaceDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.linstor.NodeConnectionData}-IO
     */
    NodeConnectionDataDatabaseDriver getNodeConnectionDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.linstor.ResourceConnectionData}-IO
     */
    ResourceConnectionDataDatabaseDriver getResourceConnectionDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.linstor.VolumeConnectionData}-IO
     */
    VolumeConnectionDataDatabaseDriver getVolumeConnectionDataDatabaseDriver();
}
