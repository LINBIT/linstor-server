package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.annotation.Nullable;

/**
 * Database driver information for IBM DB2
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class Db2DatabaseInfo implements DatabaseDriverInfo
{
    @Override
    public String jdbcUrl(String dbPath)
    {
        return "jdbc:db2:" + dbPath;
    }

    @Override
    public @Nullable String jdbcInMemoryUrl()
    {
        return null;
    }

    @Override
    public String isolationStatement()
    {
        return "SET ISOLATION SERIALIZABLE";
    }

    @Override
    public String prepareInit(String initSQL)
    {
        return initSQL;
    }
}
