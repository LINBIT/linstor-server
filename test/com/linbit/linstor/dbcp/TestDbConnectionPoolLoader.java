package com.linbit.linstor.dbcp;

import java.sql.SQLException;
import java.util.Properties;

public class TestDbConnectionPoolLoader
{
    private static final String DB_URL = "jdbc:h2:mem:testDB";
    private static final String DB_USER = "linstor";
    private static final String DB_PASSWORD = "linbit";
    private static final Properties DB_PROPS = new Properties();

    public DbConnectionPool loadDbConnectionPool()
        throws SQLException
    {
        // load the clientDriver...
        DB_PROPS.setProperty("create", "true");
        DB_PROPS.setProperty("user", DB_USER);
        DB_PROPS.setProperty("password", DB_PASSWORD);

        DbConnectionPool dbConnPool = new DbConnectionPool();
        dbConnPool.initializeDataSource(DB_URL, DB_PROPS);

        return dbConnPool;
    }
}
