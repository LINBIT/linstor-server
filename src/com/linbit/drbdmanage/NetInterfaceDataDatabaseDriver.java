package com.linbit.drbdmanage;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.ObjectDatabaseDriver;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.security.AccessDeniedException;

public interface NetInterfaceDataDatabaseDriver
{
    public ObjectDatabaseDriver<InetAddress> getNetInterfaceAddressDriver();

    public ObjectDatabaseDriver<NetInterfaceType> getNetInterfaceTypeDriver();

    public NetInterfaceData load(Connection dbCon)
        throws SQLException, AccessDeniedException;

    public void create(Connection dbCon, NetInterfaceData netData)
        throws SQLException;

}
