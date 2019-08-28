package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.NetInterfaceData;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;

/**
 * Database driver for {@link NetInterfaceData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface NetInterfaceDataDatabaseDriver extends GenericDatabaseDriver<NetInterfaceData>
{
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
