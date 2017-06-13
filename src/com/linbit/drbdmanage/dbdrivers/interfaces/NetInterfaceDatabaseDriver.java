package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.drbdmanage.NetInterface;

public interface NetInterfaceDatabaseDriver
{
    NetInterface load(Connection con, String identifier) throws SQLException;

    void create(Connection con, NetInterface netInterface) throws SQLException;

    void update(Connection con, NetInterface netInterface) throws SQLException;

    void delete(Connection con) throws SQLException;
}
