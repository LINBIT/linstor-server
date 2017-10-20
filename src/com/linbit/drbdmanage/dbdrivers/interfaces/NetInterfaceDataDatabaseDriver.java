package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.BaseTransactionObject;
import com.linbit.drbdmanage.DmIpAddress;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.NetInterfaceData;
import com.linbit.drbdmanage.NetInterfaceName;
import com.linbit.drbdmanage.Node;

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
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @return
     *  An instance which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    public NetInterfaceData load(
        Node node,
        NetInterfaceName netInterfaceName,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Persists the given {@link NetInterfaceData} into the database.
     *
     * @param netInterfaceData
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void create(NetInterfaceData netInterfaceData, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link NetInterfaceData} from the database
     *
     * @param netInterfaceData
     *  The data identifying the database entry to delete
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void delete(NetInterfaceData netInterfaceData, TransactionMgr transMgr) throws SQLException;

    /**
     * A special sub-driver to update the persisted ipAddress.
     */
    public SingleColumnDatabaseDriver<NetInterfaceData, DmIpAddress> getNetInterfaceAddressDriver();

    /**
     * A special sub-driver to update the persisted transportType.
     */
    public SingleColumnDatabaseDriver<NetInterfaceData, NetInterfaceType> getNetInterfaceTypeDriver();
}
