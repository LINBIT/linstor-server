package com.linbit.linstor.dbdrivers;

public class H2DatabaseInfo implements DatabaseDriverInfo
{
    @Override
    public String jdbcUrl(final String dbPath)
    {
        return "jdbc:h2:" + dbPath;
    }

    @Override
    public String jdbcInMemoryUrl()
    {
        return "jdbc:h2:mem:linstor";
    }

    @Override
    public String prepareInit(String initSQL)
    {
        return initSQL;
    }
}
