package com.linbit.linstor.dbdrivers;

public class DerbyDatabaseInfo implements DatabaseDriverInfo
{
    @Override
    public String jdbcUrl(final String dbPath)
    {
        return "jdbc:derby:" + dbPath + ";create=true";
    }

    @Override
    public String jdbcInMemoryUrl()
    {
        return "jdbc:derby:memory:linstor;create=true";
    }

    @Override
    public String prepareInit(String initSQL)
    {
        return initSQL;
    }
}
