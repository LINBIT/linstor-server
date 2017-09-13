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
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;

/**
 * Database driver for {@link StorPoolData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface StorPoolDataDatabaseDriver
{
    /**
     * Loads the {@link DATA} specified by the parameters {@code node} and {@code storPoolDefinition}.
     *
     * @param node
     *  Part of the primary key specifying the database entry
     * @param storPoolDefinition
     *  Part of the primary key specifying the database entry
     * @param serialGen
     *  The {@link SerialGenerator}, used to initialize the {@link SerialPropsContainer}
     * @param transMgr
     *  The {@link TransactionMgr}, used to restore references, like {@link NodeData},
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
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Persists the given {@link StorPoolData} into the database.
     *
     * @param storPoolData
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void create(StorPoolData storPool, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link StorPoolData} from the database.
     *
     * @param Data
     *  The data identifying the row to delete
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void delete(StorPoolData storPool, TransactionMgr transMgr) throws SQLException;

    /**
     * Checks if the stored primary key already exists in the database.
     * If it does not exist, {@link #create(StorPoolData, TransactionMgr)} is called.
     *
     * @param storPoolData
     *  The data identifying the row to exist
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void ensureEntryExists(StorPoolData storPool, TransactionMgr transMgr)
        throws SQLException;
}
