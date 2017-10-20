package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.BaseTransactionObject;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.VolumeConnectionData;

/**
 * Database driver for {@link com.linbit.drbdmanage.VolumeConnectionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface VolumeConnectionDataDatabaseDriver
{
    /**
     * Loads the {@link com.linbit.drbdmanage.VolumeConnectionData} specified by the parameters
     * {@code resourceName}, {@code sourceNodeName} and {@code targetNodeName}.
     * <br>
     * By convention the {@link NodeName} of @{@code sourceVolume} has to be
     * alphanumerically smaller than the {@link NodeName} of {@code targetVolume}
     * @param sourceVolume
     *  Part of the primary key specifying the database entry
     * @param targetVolume
     *  Part of the primary key specifying the database entry
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     * @param transMgr
     *  Used to restore references, like {@link com.linbit.drbdmanage.Node}, {@link com.linbit.drbdmanage.Resource}, and so on
     *
     * @return
     *  An instance which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    public VolumeConnectionData load(
        Volume sourceVolume,
        Volume targetVolume,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Loads all {@link com.linbit.drbdmanage.VolumeConnectionData} specified by the parameter
     * {@code volume}, regardless if the {@code volume} is the source of the target of the
     * specific {@link com.linbit.drbdmanage.VolumeConnection}.
     * <br>
     * @param volume
     *  Part of the primary key specifying the database entry
     * @param transMgr
     *  Used to restore references, like {@link com.linbit.drbdmanage.Node}, {@link com.linbit.drbdmanage.Resource}, and so on
     *
     * @return
     *  A list of instances which contain valid references, but are not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    public List<VolumeConnectionData> loadAllByVolume(
        Volume volume,
        TransactionMgr transMgr
    )
        throws SQLException;


    /**
     * Persists the given {@link com.linbit.drbdmanage.VolumeConnectionData} into the database.
     *
     * @param conDfnData
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void create(VolumeConnectionData conDfnData, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link com.linbit.drbdmanage.VolumeConnectionData} from the database
     *
     * @param conDfnData
     *  The data identifying the database entry to delete
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void delete(VolumeConnectionData conDfnData, TransactionMgr transMgr) throws SQLException;
}
