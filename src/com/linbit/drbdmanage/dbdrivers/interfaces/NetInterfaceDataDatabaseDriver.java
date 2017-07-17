package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.ObjectDatabaseDriver;
import com.linbit.drbdmanage.DmIpAddress;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.NetInterfaceData;
import com.linbit.drbdmanage.security.AccessDeniedException;

public interface NetInterfaceDataDatabaseDriver
{
    public ObjectDatabaseDriver<DmIpAddress> getNetInterfaceAddressDriver();

    public ObjectDatabaseDriver<NetInterfaceType> getNetInterfaceTypeDriver();

    public void create(Connection dbCon, NetInterfaceData netInterfaceData)
        throws SQLException;

    public NetInterfaceData load(Connection dbCon)
        throws SQLException, AccessDeniedException;

    public void delete(Connection con)
        throws SQLException;

}
