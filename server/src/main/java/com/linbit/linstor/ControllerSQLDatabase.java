package com.linbit.linstor;

import com.linbit.linstor.annotation.Nullable;

import java.sql.Connection;
import java.sql.SQLException;

public interface ControllerSQLDatabase extends ControllerDatabase
{
    int DEFAULT_MAX_OPEN_STMT = 100;

    void setMaxOpenPreparedStatements(int maxOpen);

    Connection getConnection() throws SQLException;

    void returnConnection(@Nullable Connection dbConn);
}
