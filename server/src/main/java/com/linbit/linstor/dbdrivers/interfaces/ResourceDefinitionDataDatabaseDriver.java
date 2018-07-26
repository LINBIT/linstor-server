package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link ResourceDefinitionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface ResourceDefinitionDataDatabaseDriver
{
    /**
     * Persists the given {@link ResourceDefinitionData} into the database.
     *
     * @param resourceDefinition
     *  The data to be stored (including the primary key)
     *
     * @throws SQLException
     */
    void create(ResourceDefinitionData resourceDefinition) throws SQLException;

    /**
     * Removes the given {@link ResourceDefinitionData} from the database
     *
     * @param resourceDefinition
     *  The data identifying the row to delete
     *
     * @throws SQLException
     */
    void delete(ResourceDefinitionData resourceDefinition) throws SQLException;

    /**
     * A special sub-driver to update the persisted flags
     */
    StateFlagsPersistence<ResourceDefinitionData> getStateFlagsPersistence();

    /**
     * A special sub-driver to update the port
     */
    SingleColumnDatabaseDriver<ResourceDefinitionData, TcpPortNumber> getPortDriver();

    /**
     * A special sub-driver to update the transport type
     */
    SingleColumnDatabaseDriver<ResourceDefinitionData, TransportType> getTransportTypeDriver();

    /**
     * Checks if the stored primary key already exists in the database.
     *
     * @param resourceName
     *  The primary key specifying the database entry
     *
     * @return
     *  True if the data record exists. False otherwise.
     * @throws SQLException
     */
    boolean exists(ResourceName resourceName) throws SQLException;

}
