package com.linbit.linstor.dbcp;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import java.sql.SQLException;
import java.util.Properties;

public class TestDbConnectionPoolModule extends AbstractModule
{
    private static final String DB_URL = "jdbc:derby:memory:testDB";
    private static final String DB_USER = "linstor";
    private static final String DB_PASSWORD = "linbit";
    private static final Properties DB_PROPS = new Properties();

    @Override
    protected void configure()
    {
    }

    @Provides
    @Singleton
    public DbConnectionPool dbConnectionPool()
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
