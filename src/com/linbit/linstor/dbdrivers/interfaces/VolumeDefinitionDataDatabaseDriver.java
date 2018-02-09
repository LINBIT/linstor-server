package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.linstor.BaseTransactionObject;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link com.linbit.linstor.VolumeDefinitionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VolumeDefinitionDataDatabaseDriver
{
    /**
     * Loads the {@link com.linbit.linstor.VolumeDefinitionData} specified by the parameter {@code resourceName} and
     * {@code volumeNumber}
     *
     * @param resourceName
     *  Part of the primary key specifying the database entry
     * @param volumeNumber
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
    VolumeDefinitionData load(
        ResourceDefinition resourceDefinition,
        VolumeNumber volumeNumber,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Persists the given {@link com.linbit.linstor.VolumeDefinitionData} into the database.
     *
     * @param volumeDefinition
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    void create(VolumeDefinitionData volumeDefinition, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link com.linbit.linstor.VolumeDefinitionData} from the database
     *
     * @param volumeDefinition
     *  The data identifying the row to delete
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    void delete(VolumeDefinitionData volumeDefinition, TransactionMgr transMgr) throws SQLException;

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<VolumeDefinitionData> getStateFlagsPersistence();

    /**
     * A special sub-driver to update the persisted minorNumber.
     */
    SingleColumnDatabaseDriver<VolumeDefinitionData, MinorNumber> getMinorNumberDriver();

    /**
     * A special sub-driver to update the persisted volumeSize.
     */
    SingleColumnDatabaseDriver<VolumeDefinitionData, Long> getVolumeSizeDriver();
}
