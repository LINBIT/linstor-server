package com.linbit.drbdmanage.dbdrivers;

import java.sql.SQLException;
import com.linbit.ServiceName;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.ResourceConnectionData;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.StorPoolDefinition;
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
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
     * Fetches all {@link com.linbit.drbdmanage.Node}s, {@link ResourceDefinition}s and {@link com.linbit.drbdmanage.StorPoolDefinition}s from
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
     * {@link com.linbit.drbdmanage.propscon.PropsContainer}-IO.
     */
    PropsConDatabaseDriver getPropsDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.drbdmanage.NodeData}-IO
     */
    NodeDataDatabaseDriver getNodeDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.drbdmanage.ResourceData}-IO
     */
    ResourceDataDatabaseDriver getResourceDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.drbdmanage.ResourceDefinitionData}-IO
     */
    ResourceDefinitionDataDatabaseDriver getResourceDefinitionDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.drbdmanage.VolumeData}-IO
     */
    VolumeDataDatabaseDriver getVolumeDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.drbdmanage.VolumeDefinitionData}-IO
     */
    VolumeDefinitionDataDatabaseDriver getVolumeDefinitionDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.drbdmanage.StorPoolDefinitionData}-IO
     */
    StorPoolDefinitionDataDatabaseDriver getStorPoolDefinitionDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.drbdmanage.StorPoolData}-IO
     */
    StorPoolDataDatabaseDriver getStorPoolDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.drbdmanage.NetInterfaceData}-IO
     */
    NetInterfaceDataDatabaseDriver getNetInterfaceDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.drbdmanage.NodeConnectionData}-IO
     */
    NodeConnectionDataDatabaseDriver getNodeConnectionDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.drbdmanage.ResourceConnectionData}-IO
     */
    ResourceConnectionDataDatabaseDriver getResourceConnectionDataDatabaseDriver();

    /**
     * Returns the database driver specific implementation for
     * {@link com.linbit.drbdmanage.VolumeConnectionData}-IO
     */
    VolumeConnectionDataDatabaseDriver getVolumeConnectionDataDatabaseDriver();
}
