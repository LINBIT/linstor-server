package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterface.EncryptionType;
import com.linbit.linstor.NetInterfaceData;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.TcpPortNumber;

/**
 * Database driver for {@link NetInterfaceData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface NetInterfaceDataDatabaseDriver
{
    /**
     * Loads the {@link NetInterfaceData} specified by the parameters {@code node} and
     * {@code netInterfaceName}.
     *
     * @param node
     *  Part of the primary key specifying the database entry
     * @param netInterfaceName
     *  Part of the primary key specifying the database entry
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     *
     * @throws SQLException
     */
    NetInterfaceData load(
        Node node,
        NetInterfaceName netInterfaceName,
        boolean logWarnIfNotExists
    )
        throws SQLException;

    /**
     * Persists the given {@link NetInterfaceData} into the database.
     *
     * @param netInterfaceData
     *  The data to be stored (including the primary key)
     * @throws SQLException
     */
    void create(NetInterfaceData netInterfaceData) throws SQLException;

    /**
     * Removes the given {@link NetInterfaceData} from the database
     *
     * @param netInterfaceData
     *  The data identifying the database entry to delete
     * @throws SQLException
     */
    void delete(NetInterfaceData netInterfaceData) throws SQLException;

    /**
     * A special sub-driver to update the persisted ipAddress.
     */
    SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress> getNetInterfaceAddressDriver();

    /**
     * A special sub-driver to update the persisted satellite connection port.
     */
    SingleColumnDatabaseDriver<NetInterfaceData, TcpPortNumber> getStltConnPortDriver();

    /**
     * A special sub-driver to update the persisted satellite connection port.
     */
    SingleColumnDatabaseDriver<NetInterfaceData, EncryptionType> getStltConnEncrTypeDriver();
}
