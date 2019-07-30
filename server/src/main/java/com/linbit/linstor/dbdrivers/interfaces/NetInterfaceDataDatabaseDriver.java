package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.core.objects.NetInterfaceData;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.dbdrivers.DatabaseException;

/**
 * Database driver for {@link NetInterfaceData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface NetInterfaceDataDatabaseDriver
{
    /**
     * Persists the given {@link NetInterfaceData} into the database.
     *
     * @param netInterfaceData
     *  The data to be stored (including the primary key)
     * @throws DatabaseException
     */
    void create(NetInterfaceData netInterfaceData) throws DatabaseException;

    /**
     * Removes the given {@link NetInterfaceData} from the database
     *
     * @param netInterfaceData
     *  The data identifying the database entry to delete
     * @throws DatabaseException
     */
    void delete(NetInterfaceData netInterfaceData) throws DatabaseException;

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
