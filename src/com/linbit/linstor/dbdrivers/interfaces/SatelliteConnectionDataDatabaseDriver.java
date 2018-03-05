package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.Node;
import com.linbit.linstor.SatelliteConnection;
import com.linbit.linstor.SatelliteConnectionData;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.SatelliteConnection.EncryptionType;
import com.linbit.linstor.transaction.BaseTransactionObject;

public interface SatelliteConnectionDataDatabaseDriver
{
    /**
     * Loads the {@link SatelliteconnectionData} specified by the parameter {@code node}.
     *
     * @param node
     *  The primary key specifying the database entry
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     *
     * @return
     *  An instance which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    SatelliteConnectionData load(Node node, boolean logWarnIfNotExists) throws SQLException;

    /**
     * Persists the given {@link SatelliteConnectionData} into the database.
     *
     * @param satelliteConnectionData
     *  The data to be stored (including the primary key)
     *
     * @throws SQLException
     */
    void create(SatelliteConnection satelliteConnectionData) throws SQLException;

    /**
     * Removes the given {@link SatelliteConnectionData} from the database
     *
     * @param satelliteConnectionData
     *  The data identifying the database entry to delete
     *
     * @throws SQLException
     */
    void delete(SatelliteConnection satelliteConnectionData) throws SQLException;

    /**
     * A special sub-driver to update the persisted port.
     */
    SingleColumnDatabaseDriver<SatelliteConnectionData, TcpPortNumber> getSatelliteConnectionPortDriver();

    /**
     * A special sub-driver to update the persisted type.
     */
    SingleColumnDatabaseDriver<SatelliteConnectionData, EncryptionType> getSatelliteConnectionTypeDriver();
}
