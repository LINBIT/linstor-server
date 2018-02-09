package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.linstor.BaseTransactionObject;
import com.linbit.linstor.Resource;
import com.linbit.linstor.VolumeData;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link com.linbit.linstor.VolumeData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VolumeDataDatabaseDriver
{
    /**
     * Loads the {@link com.linbit.linstor.VolumeData} specified by the parameters
     *
     * @param resource
     *  Part of the primary key specifying the database entry
     * @param volumeDefinition
     *  Part of the primary key specifying the database entry
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr}, used to restore references, like {@link com.linbit.linstor.Node},
     *  {@link com.linbit.linstor.Resource}, and so on
     * @return
     *  An instance which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    VolumeData load(
        Resource resource,
        VolumeDefinition volumeDefinition,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Persists the given {@link com.linbit.linstor.VolumeData} into the database.
     *
     * @param volume
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    void create(VolumeData volume, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link com.linbit.linstor.VolumeData} from the database
     *
     * @param volume
     *  The data identifying the row to delete
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    void delete(VolumeData volume, TransactionMgr transMgr) throws SQLException;

    /**
     * A special sub-driver to update the persisted flags
     */
    StateFlagsPersistence<VolumeData> getStateFlagsPersistence();
}
