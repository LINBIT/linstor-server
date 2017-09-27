package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.BaseTransactionObject;
import com.linbit.drbdmanage.StorPoolDefinitionData;
import com.linbit.drbdmanage.StorPoolName;

/**
 * Database driver for {@link StorPoolDefinitionData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface StorPoolDefinitionDataDatabaseDriver
{
    /**
     * Loads the {@link StorPoolDefinitionData} specified by the parameter {@code storPoolName}
     *
     * @param storPoolName
     *  The primaryKey identifying the row to load
     * @param logWarnIfNoTExists
     *  If true a warning is logged if the requested entry does not exist
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @return
     *  An instance which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    public StorPoolDefinitionData load(
        StorPoolName storPoolName,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Persists the given {@link StorPoolDefinitionData} into the database.
     *
     * @param storPoolDefinitionData
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void create(StorPoolDefinitionData storPoolDefinition, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link StorPoolDefinitionData} from the database.
     *
     * @param storPoolDefinitionData
     *  The data identifying the row to delete
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void delete(StorPoolDefinitionData storPoolDefinition, TransactionMgr transMgr) throws SQLException;
}
