package com.linbit.linstor.dbdrivers;

public class H2DatabaseInfo implements DatabaseDriverInfo
{
    public H2DatabaseInfo()
    {
        DatabaseDriverInfo.loadDriver("org.h2.Driver");
    }

    @Override
    public String jdbcUrl(final String dbPath)
    {
        return "jdbc:h2:" + dbPath + ";SCHEMA_SEARCH_PATH=LINSTOR";
    }

    @Override
    public String jdbcInMemoryUrl()
    {
        return "jdbc:h2:mem:linstor;SCHEMA_SEARCH_PATH=LINSTOR";
    }

    @Override
    public String isolationStatement()
    {
        return "SET LOCK_MODE 1;";
    }

    @Override
    public String prepareInit(String initSQL)
    {
        return initSQL.replace("UUID CHAR(16) FOR BIT DATA", "UUID UUID");
    }
}
