package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.BaseTransactionObject;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link VolumeDefinitionData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface VolumeDefinitionDataDatabaseDriver
{
    /**
     * Loads the {@link VolumeDefinitionData} specified by the parameter {@code resourceName} and
     * {@code volumeNumber}
     *
     * @param resourceName
     *  Part of the primary key specifying the database entry
     * @param volumeNumber
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
    public VolumeDefinitionData load(
        ResourceDefinition resourceDefinition,
        VolumeNumber volumeNumber,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Persists the given {@link VolumeDefinitionData} into the database.
     *
     * @param volumeDefinition
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void create(VolumeDefinitionData volumeDefinition, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link VolumeDefinitionData} from the database
     *
     * @param volumeDefinition
     *  The data identifying the row to delete
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void delete(VolumeDefinitionData volumeDefinition, TransactionMgr transMgr) throws SQLException;

    /**
     * A special sub-driver to update the persisted flags.
     */
    public StateFlagsPersistence<VolumeDefinitionData> getStateFlagsPersistence();

    /**
     * A special sub-driver to update the persisted minorNumber.
     */
    public SingleColumnDatabaseDriver<VolumeDefinitionData, MinorNumber> getMinorNumberDriver();

    /**
     * A special sub-driver to update the persisted volumeSize.
     */
    public SingleColumnDatabaseDriver<VolumeDefinitionData, Long> getVolumeSizeDriver();
}
