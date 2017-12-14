package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.linstor.BaseTransactionObject;
import com.linbit.linstor.Node;
import com.linbit.linstor.SatelliteConnection;
import com.linbit.linstor.SatelliteConnectionData;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.SatelliteConnection.EncryptionType;

public interface SatelliteConnectionDataDatabaseDriver
{
    /**
     * Loads the {@link SatelliteconnectionData} specified by the parameter {@code node}.
     *
     * @param node
     *  The primary key specifying the database entry
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @return
     *  An instance which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    public SatelliteConnectionData load(
        Node node,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Persists the given {@link SatelliteConnectionData} into the database.
     *
     * @param satelliteConnectionData
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void create(SatelliteConnection satelliteConnectionData, TransactionMgr transMgr)
        throws SQLException;

    /**
     * Removes the given {@link SatelliteConnectionData} from the database
     *
     * @param satelliteConnectionData
     *  The data identifying the database entry to delete
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void delete(SatelliteConnection satelliteConnectionData, TransactionMgr transMgr)
        throws SQLException;

    /**
     * A special sub-driver to update the persisted port.
     */
    public SingleColumnDatabaseDriver<SatelliteConnectionData, TcpPortNumber> getSatelliteConnectionPortDriver();

    /**
     * A special sub-driver to update the persisted type.
     */
    public SingleColumnDatabaseDriver<SatelliteConnectionData, EncryptionType> getSatelliteConnectionTypeDriver();
}
