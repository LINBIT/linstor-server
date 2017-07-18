package com.linbit.drbdmanage.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.drbdmanage.DmIpAddress;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.NetInterfaceData;
import com.linbit.drbdmanage.dbdrivers.interfaces.BaseDatabaseDriver.BaseSimpleDatabaseDriver;

/**
 * Database driver for {@link NetInterfaceData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface NetInterfaceDataDatabaseDriver extends BaseSimpleDatabaseDriver<NetInterfaceData>
{
    /**
     * A special sub-driver to update the persisted ipAddress. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public SingleColumnDatabaseDriver<DmIpAddress> getNetInterfaceAddressDriver();

    /**
     * A special sub-driver to update the persisted transportType. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public SingleColumnDatabaseDriver<NetInterfaceType> getNetInterfaceTypeDriver();
}
