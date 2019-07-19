package com.linbit.linstor.dbcp;

import java.sql.SQLException;
import java.util.Properties;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.DatabaseInfoImpl;
import com.linbit.linstor.core.LinstorConfigToml;
import com.linbit.linstor.dbcp.migration.MigrationUtils;

public class TestDbConnectionPoolLoader
{
    private static final String DB_TYPE = "h2";
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

        DatabaseInfo dbInfo = new DatabaseInfoImpl();
        DbConnectionPool dbConnPool = new DbConnectionPool(dbInfo, new LinstorConfigToml());
        dbConnPool.initializeDataSource(DB_URL);
        MigrationUtils.setDatabaseInfo(dbInfo);

        return dbConnPool;
    }

    public String getDbType()
    {
        return DB_TYPE;
    }
}
