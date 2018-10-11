package com.linbit.linstor.dbdrivers;

public class PostgresqlDatabaseInfo implements DatabaseDriverInfo
{
    @Override
    public String jdbcUrl(String dbPath)
    {
        return "jdbc:postgresql:" + dbPath;
    }

    @Override
    public String jdbcInMemoryUrl()
    {
        return null;
    }

    @Override
    public String isolationStatement()
    {
        return "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;";
    }

    @Override
    public String prepareInit(String initSQL)
    {
        return initSQL;
    }
}
