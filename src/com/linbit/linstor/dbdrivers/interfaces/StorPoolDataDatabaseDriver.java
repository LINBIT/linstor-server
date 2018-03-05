package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;

import com.linbit.linstor.Node;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

/**
 * Database driver for {@link com.linbit.linstor.StorPoolData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface StorPoolDataDatabaseDriver
{
    /**
     * Loads the {@link com.linbit.linstor.StorPool} specified by the parameters
     * {@code node} and {@code storPoolDefinition}.
     *
     * @param node
     *  Part of the primary key specifying the database entry
     * @param storPoolDefinition
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
    StorPoolData load(Node node, StorPoolDefinition storPoolDefinition, boolean logWarnIfNotExists)
        throws SQLException;

    /**
     * Persists the given {@link com.linbit.linstor.StorPoolData} into the database.
     *
     * @param storPool
     *  The data to be stored (including the primary key)
     *
     * @throws SQLException
     */
    void create(StorPoolData storPool) throws SQLException;

    /**
     * Removes the given {@link com.linbit.linstor.StorPoolData} from the database.
     *
     * @param storPool
     *  The data identifying the row to delete
     *
     * @throws SQLException
     */
    void delete(StorPoolData storPool) throws SQLException;

    /**
     * Checks if the stored primary key already exists in the database.
     * If it does not exist, {@link StorPoolDefinitionDataDatabaseDriver#create(StorPoolData, TransactionMgr)}
     * is called.
     *
     * @param storPool
     *  The data identifying the row to exist
     *
     * @throws SQLException
     */
    void ensureEntryExists(StorPoolData storPool)
        throws SQLException;
}
