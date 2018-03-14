package com.linbit.linstor.dbdrivers;

public interface DatabaseDriverInfo
{
    static void loadDriver(final String driverClass)
    {
        try
        {
            Class.forName(driverClass).newInstance();
        }
        catch (final ClassNotFoundException | IllegalAccessException | InstantiationException ce)
        {
            System.err.println("Database driver could not be loaded.");
        }
    }

    static DatabaseDriverInfo CreateDriverInfo(final String dbType)
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
