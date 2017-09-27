package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.drbdmanage.BaseTransactionObject;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.ResourceDefinitionData;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link ResourceDefinitionData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface ResourceDefinitionDataDatabaseDriver
{
    /**
     * Loads the {@link ResourceDefinitionData} specified by the parameter {@code resuorceName}
     *
     * @param resourceName
     *  The primary key specifying the database entry
     * @param logWarnIfNoTExists
     *  If true a warning is logged if the requested entry does not exist
     * @param transMgr
     *  The {@link TransactionMgr}, used to restore references, like {@link Node},
     *  {@link Resource}, and so on
     * @return
     *  An instance which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    public ResourceDefinitionData load(
        ResourceName resourceName,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Persists the given {@link ResourceDefinitionData} into the database.
     *
     * @param resourceDefinition
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void create(ResourceDefinitionData resourceDefinition, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link ResourceDefinitionData} from the database
     *
     * @param resourceDefinition
     *  The data identifying the row to delete
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void delete(ResourceDefinitionData resourceDefinition, TransactionMgr transMgr) throws SQLException;

    /**
     * A special sub-driver to update the persisted flags
     */
    public StateFlagsPersistence<ResourceDefinitionData> getStateFlagsPersistence();

    /**
     * Checks if the stored primary key already exists in the database.
     *
     * @param resourceName
     *  The primary key specifying the database entry
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @return
     *  True if the data record exists. False otherwise.
     * @throws SQLException
     */
    public boolean exists(ResourceName resourceName, TransactionMgr transMgr)
        throws SQLException;
}
