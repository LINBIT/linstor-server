package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.annotation.Nullable;

public class MariaDBInfo implements DatabaseDriverInfo
{
    private String compatType = "mariadb";

    public MariaDBInfo(String type)
    {
        compatType = type;
    }

    @Override
    public String jdbcUrl(String dbPath)
    {
        return "jdbc:" + compatType + ":" + dbPath;
    }

    @Override
    public @Nullable String jdbcInMemoryUrl()
    {
        return null;
    }

    @Override
    public String isolationStatement()
    {
        return "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE";
    }

    @Override
    public String prepareInit(String initSQL)
    {
        return initSQL;
    }
}
