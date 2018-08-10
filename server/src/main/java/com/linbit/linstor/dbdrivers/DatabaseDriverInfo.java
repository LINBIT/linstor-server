package com.linbit.linstor.dbdrivers;

public interface DatabaseDriverInfo
{
    static DatabaseDriverInfo createDriverInfo(final String dbType)
    {
        DatabaseDriverInfo dbdriver = null;
        switch (dbType)
        {
            case "h2":
                dbdriver = new H2DatabaseInfo();
                break;
            case "derby":
                dbdriver = new DerbyDatabaseInfo();
                break;
            case "postgresql":
                dbdriver = new PostgresqlDatabaseInfo();
                break;
            case "mysql":
            case "mariadb":
                dbdriver = new MariaDBInfo(dbType);
                break;
            default:
                break;
        }

        return dbdriver;
    }

    String jdbcUrl(String dbPath);
    String jdbcInMemoryUrl();

    String isolationStatement();

    String prepareInit(String initSQL);
}
