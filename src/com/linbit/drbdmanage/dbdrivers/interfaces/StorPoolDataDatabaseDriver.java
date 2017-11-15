package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.BaseTransactionObject;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.StorPoolData;
import com.linbit.drbdmanage.StorPoolDefinition;

/**
 * Database driver for {@link com.linbit.drbdmanage.StorPoolData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface StorPoolDataDatabaseDriver
{
    /**
     * Loads the {@link com.linbit.drbd.ApplyStorPool} specified by the parameters
     * {@code node} and {@code storPoolDefinition}.
     *
     * @param node
     *  Part of the primary key specifying the database entry
     * @param storPoolDefinition
     *  Part of the primary key specifying the database entry
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr}, used to restore references, like {@link NodeData},
     *  {@link ResourceData}, and so on
     * @return
     *  An instance which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    public StorPoolData load(
        Node node,
        StorPoolDefinition storPoolDefinition,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Persists the given {@link com.linbit.drbdmanage.StorPoolData} into the database.
     *
     * @param storPool
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void create(StorPoolData storPool, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link com.linbit.drbdmanage.StorPoolData} from the database.
     *
     * @param storPool
     *  The data identifying the row to delete
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void delete(StorPoolData storPool, TransactionMgr transMgr) throws SQLException;

    /**
     * Checks if the stored primary key already exists in the database.
     * If it does not exist, {@link StorPoolDefinitionDataDatabaseDriver#create(StorPoolData, TransactionMgr)}
     * is called.
     *
     * @param storPool
     *  The data identifying the row to exist
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void ensureEntryExists(StorPoolData storPool, TransactionMgr transMgr)
        throws SQLException;
}
