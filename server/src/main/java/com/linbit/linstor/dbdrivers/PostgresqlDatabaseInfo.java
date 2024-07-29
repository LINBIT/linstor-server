package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.annotation.Nullable;

public class PostgresqlDatabaseInfo implements DatabaseDriverInfo
{
    @Override
    public String jdbcUrl(String dbPath)
    {
        return "jdbc:postgresql:" + dbPath;
    }

    @Override
    public @Nullable String jdbcInMemoryUrl()
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
