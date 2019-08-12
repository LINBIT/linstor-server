package com.linbit.linstor;

import java.sql.Connection;
import java.sql.SQLException;

public interface ControllerSQLDatabase extends ControllerDatabase
{
    int DEFAULT_MAX_OPEN_STMT = 100;

    void setMaxOpenPreparedStatements(int maxOpen);

    Connection getConnection() throws SQLException;

    // Must be able to handle dbConn == null as a valid input
    void returnConnection(Connection dbConn);
}
