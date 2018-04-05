package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;

import com.linbit.linstor.Node;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link ResourceData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface ResourceDataDatabaseDriver
{
    /**
     * Loads the {@link ResourceData} specified by the parameters {@code node} and
     * {@code resourceName}.
     *
     * @param node
     *  Part of the primary key specifying the database entry
     * @param resourceName
     *  Part of the primary key specifying the database entry
     *  If true a warning is logged if the requested entry does not exist
     *
     * @throws SQLException
     */
    ResourceData load(Node node, ResourceName resourceName, boolean logWarnIfNotExists)
        throws SQLException;

    /**
     * Persists the given {@link ResourceData} into the database.
     *
     * The primary key for the insert statement is stored as
     * instance variables already, thus might not be retrieved from the
     * conDfnData parameter.
     *
     * @param resource
     *  The data to be stored (including the primary key)
     *
     * @throws SQLException
     */
    void create(ResourceData resource) throws SQLException;

    /**
     * Removes the given {@link ResourceData} from the database.
     *
     * @param resource
     *  The data identifying the row to delete
     *
     * @throws SQLException
     */
    void delete(ResourceData resource) throws SQLException;

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<ResourceData> getStateFlagPersistence();
}
