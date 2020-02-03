package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

/**
 * Database driver for {@link NetInterface}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface NetInterfaceDatabaseDriver extends GenericDatabaseDriver<NetInterface>
{
    /**
     * A special sub-driver to update the persisted ipAddress.
     */
    SingleColumnDatabaseDriver<NetInterface, LsIpAddress> getNetInterfaceAddressDriver();

    /**
     * A special sub-driver to update the persisted satellite connection port.
     */
    SingleColumnDatabaseDriver<NetInterface, TcpPortNumber> getStltConnPortDriver();

    /**
     * A special sub-driver to update the persisted satellite connection port.
     */
    SingleColumnDatabaseDriver<NetInterface, EncryptionType> getStltConnEncrTypeDriver();
}
