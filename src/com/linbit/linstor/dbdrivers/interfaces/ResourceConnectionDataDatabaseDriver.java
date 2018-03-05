package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;
import java.util.List;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceConnectionData;
import com.linbit.linstor.transaction.BaseTransactionObject;

/**
 * Database driver for {@link ResourceConnectionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface ResourceConnectionDataDatabaseDriver
{
    /**
     * Loads the {@link ResourceConnectionData} specified by the parameters
     * {@code source} and {@code target}.
     * <br>
     * By convention the {@link NodeName} of the node where {@code source} is assigned has to be
     * alphanumerically smaller than the {@link NodeName} of {@code target}' node.
     * @param source
     *  Part of the primary key specifying the database entry
     * @param target
     *  Part of the primary key specifying the database entry
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     *
     * @return
     *  An instance which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    ResourceConnectionData load(Resource source, Resource target, boolean logWarnIfNotExists)
        throws SQLException;

    /**
     * Loads all {@link ResourceConnectionData} specified by the parameter
     * {@code resource}, regardless if the {@code resource} is the source of the target of the
     * specific {@link ResourceConnection}.
     * <br>
     * @param resource
     *  Part of the primary key specifying the database entry
     *
     * @return
     *  A list of instances which contain valid references, but are not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    List<ResourceConnectionData> loadAllByResource(Resource resource) throws SQLException;

    /**
     * Persists the given {@link ResourceConnectionData} into the database.
     *
     * @param resConDfnData
     *  The data to be stored (including the primary key)
     * @throws SQLException
     */
    void create(ResourceConnectionData resConDfnData) throws SQLException;

    /**
     * Removes the given {@link ResourceConnectionData} from the database
     *
     * @param resConDfnData
     *  The data identifying the database entry to delete
     * @throws SQLException
     */
    void delete(ResourceConnectionData resConDfnData) throws SQLException;
}
