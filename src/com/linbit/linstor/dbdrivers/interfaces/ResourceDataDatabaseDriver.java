package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.linstor.BaseTransactionObject;
import com.linbit.linstor.Node;
import com.linbit.linstor.ResourceConnectionData;
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
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr}, used to restore references, like {@link com.linbit.linstor.Node},
     *  {@link com.linbit.linstor.Resource}, and so on
     * @return
     *  A {@link ResourceConnectionData} which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    ResourceData load(
        Node node,
        ResourceName resourceName,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
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
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    void create(ResourceData resource, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link ResourceData} from the database.
     *
     * @param resource
     *  The data identifying the row to delete
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    void delete(ResourceData resource, TransactionMgr transMgr) throws SQLException;

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<ResourceData> getStateFlagPersistence();
}
