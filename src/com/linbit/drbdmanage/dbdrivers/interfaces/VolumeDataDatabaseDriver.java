package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.BaseTransactionObject;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link VolumeData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface VolumeDataDatabaseDriver
{
    /**
     * Loads the {@link VolumeData} specified by the parameters
     *
     * @param resource
     *  Part of the primary key specifying the database entry
     * @param volumeDefinition
     *  Part of the primary key specifying the database entry
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
    public VolumeData load(
        Resource resource,
        VolumeDefinition volumeDefinition,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Persists the given {@link VolumeData} into the database.
     *
     * @param volume
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void create(VolumeData volume, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link VolumeData} from the database
     *
     * @param volume
     *  The data identifying the row to delete
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void delete(VolumeData volume, TransactionMgr transMgr) throws SQLException;

    /**
     * A special sub-driver to update the persisted flags
     */
    public StateFlagsPersistence<VolumeData> getStateFlagsPersistence();
}
